# Design

Status: **Phases 0-5 complete** (scaffold, submit API, claim + execute, retries + DLQ, crash recovery + timeouts, cron schedules; last updated 2026-11-17). Phase 6 in §10 is
designed but not built; this document is updated as each phase lands.

Pinned versions: Java 21, Spring Boot 4.1.2, PostgreSQL 16 (`postgres:16-alpine` in both
`compose.yaml` and the Testcontainers config, so tests run against what Compose runs).

## 1. Overview

A job scheduler where PostgreSQL is the only piece of infrastructure: it is the queue,
the lock manager, and the source of truth. Clients submit jobs over REST; a pool of
identical worker processes claims and executes them.

```
            ┌──────────┐   POST /api/v1/jobs
 client ───▶│   API    │──────────────┐
            └──────────┘              ▼
                               ┌─────────────┐
 ┌──────────┐  claim / ack     │ PostgreSQL  │
 │ worker 1 │◀────────────────▶│  jobs       │
 ├──────────┤  heartbeat       │  workers    │
 │ worker 2 │◀────────────────▶│  schedules  │
 ├──────────┤                  │  job_attempts│
 │ worker N │◀────────────────▶└─────────────┘
 └──────────┘
   each worker also runs: reaper loop, cron-firing loop
```

**Guarantee: at-least-once execution.** A job may run more than once (a worker can crash
after doing the work but before recording success), so handlers must be idempotent.
Exactly-once is not achievable without the handler's side effects sharing the job's
transaction, and we say so rather than pretend.

### Why Postgres as the queue (and not Redis/Kafka/RabbitMQ)
- Job submission can be transactional with the submitter's own data, and state
  transitions are ACID. No dual-write problem.
- `FOR UPDATE SKIP LOCKED` gives contention-free multi-consumer claiming.
- Scheduled (`run_at`), prioritised, queryable, cancellable jobs are just SQL; brokers
  are poor at "run this in 3 hours" and "show me everything that failed".
- One dependency to operate. The trade-off is throughput: this design tops out in the
  low thousands of jobs/sec, which is far beyond what most systems need.

## 2. Process model

One Spring Boot artifact, with roles switched by config:

| Property | Effect |
|---|---|
| `scheduler.api.enabled` | Serves the REST API |
| `scheduler.worker.enabled` | Runs the poll/execute loop, heartbeat, reaper, cron firer |

Docker Compose runs one `api` container and N `worker` containers from the same image.
One artifact keeps the build simple while still demonstrating independent scaling.

There is **no leader election**. The reaper and the cron firer run on every worker and
are made safe by row locks and idempotent SQL, so any number of instances can run them
concurrently. This removes a whole class of failure (leader dies, nobody reaps).

## 3. Database schema

```sql
CREATE TABLE jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type             text        NOT NULL,            -- selects the handler
    payload          jsonb       NOT NULL DEFAULT '{}',
    priority         int         NOT NULL DEFAULT 0,  -- higher runs first
    status           text        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','DEAD','CANCELLED')),
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int         NOT NULL DEFAULT 0,  -- claim counter: +1 per claim, NEVER decremented or reset
    attempts_excused int         NOT NULL DEFAULT 0,  -- claims that do not count against the retry budget
    max_retries      int         NOT NULL DEFAULT 3,  -- DEAD when attempts - attempts_excused > max_retries
    timeout_seconds  int         NOT NULL DEFAULT 300 CHECK (timeout_seconds > 0),
    CHECK (attempts_excused <= attempts),
    idempotency_key  text,
    locked_by        uuid REFERENCES workers(id),
    locked_at        timestamptz,
    last_error       text,
    schedule_id      uuid REFERENCES schedules(id) ON DELETE SET NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- Claim query: index-ordered scan over only the claimable rows.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at, id) WHERE status = 'PENDING';
-- Reaper: find jobs held by a given worker.
CREATE INDEX jobs_running_idx ON jobs (locked_by) WHERE status = 'RUNNING';
-- Idempotent submission.
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
-- DLQ listing.
CREATE INDEX jobs_dead_idx ON jobs (finished_at DESC) WHERE status = 'DEAD';
-- Keyset pagination for GET /api/v1/jobs.
CREATE INDEX jobs_list_idx ON jobs (created_at DESC, id DESC);

CREATE TABLE workers (
    id                uuid PRIMARY KEY,               -- generated per process start
    hostname          text        NOT NULL,
    started_at        timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    status            text        NOT NULL DEFAULT 'ALIVE'
                      CHECK (status IN ('ALIVE','STOPPED','DEAD'))
);

-- Reaper: expired ALIVE workers, without scanning old STOPPED/DEAD rows.
CREATE INDEX workers_alive_idx ON workers (last_heartbeat_at) WHERE status = 'ALIVE';

CREATE TABLE schedules (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name          text        NOT NULL UNIQUE,
    cron          text        NOT NULL,               -- Spring 6-field cron
    timezone      text        NOT NULL DEFAULT 'UTC',
    job_type      text        NOT NULL,
    payload       jsonb       NOT NULL DEFAULT '{}',
    priority      int         NOT NULL DEFAULT 0,
    max_retries   int         NOT NULL DEFAULT 3,
    timeout_seconds int       NOT NULL DEFAULT 300,
    enabled       boolean     NOT NULL DEFAULT true,
    next_fire_at  timestamptz NOT NULL,
    last_fired_at timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE enabled;
-- Lets ON DELETE SET NULL find a schedule's jobs without a table scan.
CREATE INDEX jobs_schedule_idx ON jobs (schedule_id) WHERE schedule_id IS NOT NULL;

CREATE TABLE job_attempts (                           -- append-only execution history
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     int  NOT NULL,
    worker_id   uuid NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    outcome     text CHECK (outcome IN ('SUCCEEDED','FAILED','TIMED_OUT','ABANDONED','RELEASED')),
    error       text,
    UNIQUE (job_id, attempt)
);
```

The DDL above is the target schema. Migrations build it up phase by phase: `V2` creates
`jobs` without the `locked_by` / `schedule_id` foreign keys, which are added by the
migrations that create `workers` (Phase 2) and `schedules` (Phase 5).

Schema reasoning:
- **Partial indexes** keep the hot-path indexes proportional to the *active* queue, not
  to the ever-growing history of finished jobs.
- **`status` as `text` + CHECK**, not a Postgres enum: adding a state is a one-line
  migration, and enums cannot have values removed.
- **`attempts` is a monotonic claim counter, separate from the retry budget.** It is
  the fencing token (§5) and the key of `job_attempts (job_id, attempt)`, so a value
  must never be issued twice for the same job. Two operations want to "give an attempt
  back": graceful-shutdown release (§7) and DLQ requeue (§6). Decrementing or resetting
  `attempts` there would make the next claim reuse a number, which violates
  `UNIQUE (job_id, attempt)` on insert and reissues a fencing token. Instead those
  operations raise `attempts_excused`, and the budget is computed as
  **`attempts - attempts_excused`** ("counted attempts"). Release does
  `attempts_excused + 1`; requeue does `attempts_excused = attempts`. History stays
  complete: an excused attempt still has its own `job_attempts` row.
- **`job_attempts`** is separate from `jobs` so the job row stays small and hot while
  history is queryable for debugging and metrics.
- **`type`** is not in the original feature list but is required: workers need to know
  which handler to invoke for a payload.

## 4. Job state machine

```
                 submit
                   │
                   ▼
   ┌──────────▶ PENDING ──────cancel─────▶ CANCELLED
   │               │
   │             claim (attempts++)
   │               ▼
   │            RUNNING ──────success────▶ SUCCEEDED
   │               │
   │   ┌───────────┴────────────┐
   │ failure / timeout /        │ failure / timeout /
   │ worker died, budget left   │ worker died, budget exhausted
   │ (run_at = now+backoff)     ▼
   ├───┘                       DEAD ──requeue (attempts_excused = attempts)──▶ PENDING
   │
   └── graceful release from RUNNING (attempts_excused++, run_at unchanged)
```

"Budget exhausted" means `attempts - attempts_excused > max_retries`.

| From | To | Trigger | Guard in the `UPDATE ... WHERE` |
|---|---|---|---|
| — | PENDING | API submit / cron fire | unique idempotency key |
| PENDING | RUNNING | worker claim | `status='PENDING' AND run_at <= now()`, row lock |
| RUNNING | SUCCEEDED | handler returned | `status='RUNNING' AND locked_by=? AND attempts=?` |
| RUNNING | PENDING | handler threw, retries left | same fencing; sets `run_at` |
| RUNNING | DEAD | handler threw, none left | same fencing |
| RUNNING | PENDING / DEAD | handler exceeded `timeout_seconds` (§7) | same fencing; counted like a failure |
| RUNNING | PENDING / DEAD | reaper: owner's heartbeat expired | `status='RUNNING' AND locked_by=<dead worker>` |
| RUNNING | PENDING | graceful shutdown release | same fencing; `attempts_excused + 1` |
| PENDING | CANCELLED | API | `status='PENDING'` |
| DEAD | PENDING | API requeue | `status='DEAD'`; `attempts_excused = attempts` |

SUCCEEDED and CANCELLED are terminal. Retrying jobs go back to PENDING with a future
`run_at` instead of a separate RETRYING/SCHEDULED state: "waiting to run" is one concept
and the claim query already handles it with `run_at <= now()`. Fewer states, fewer
transitions to get wrong.

## 5. Claiming and executing

```sql
WITH picked AS MATERIALIZED (
    SELECT id FROM jobs
    WHERE status = 'PENDING' AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT :free_slots
    FOR UPDATE SKIP LOCKED
), claimed AS (
    UPDATE jobs j
    SET status = 'RUNNING', locked_by = :worker, locked_at = now(),
        attempts = j.attempts + 1, updated_at = now()
    FROM picked WHERE j.id = picked.id
    RETURNING j.*
), recorded AS (
    INSERT INTO job_attempts (job_id, attempt, worker_id, started_at)
    SELECT id, attempts, :worker, now() FROM claimed
)
SELECT * FROM claimed ORDER BY priority DESC, run_at, id;
```

- **One statement does everything**: lock, transition, and write the `job_attempts` row.
  It is its own implicit transaction, so a job can never be RUNNING without a history
  row, and there is no window between "locked" and "marked".
- **A materialized CTE, not `WHERE id IN (subquery)`**: with `IN`, the planner is free to
  pick a plan that evaluates the locking subquery more than once, which can lock and
  update more rows than `LIMIT`. The CTE runs exactly once.
- **Acks use the same shape**: one statement updates `jobs` under the fencing guard and
  closes the matching `job_attempts` row; its row count tells the worker whether it
  still owned the job.

- **`SKIP LOCKED`**: a worker skips rows another worker's in-flight claim has locked
  rather than queueing behind them. Without it, all workers serialise on the head of
  the queue; with plain `FOR UPDATE` they would block and then find the row already taken.
- **Claim is a short transaction; the handler runs outside any transaction.** Holding a
  row lock for the duration of a job would pin a connection per running job, hold back
  vacuum, and make the lock *be* the lease, which dies silently with the connection.
  Instead the durable `RUNNING` + `locked_by` columns record ownership.
- **Batch size = free executor slots**, so a worker never claims work it cannot start
  immediately (claimed-but-idle jobs would be invisible to other, idle workers).
- **Attempt counted at claim, not at failure**: a job that kills its worker (OOM,
  `System.exit`) still consumes an attempt, so a poison pill ends up in the DLQ instead
  of crash-looping the fleet forever.
- **Fencing**: completion updates carry `locked_by = :worker AND attempts = :attempt`.
  If a worker stalls (long GC pause), gets reaped, and wakes up later, its ack matches
  zero rows and is discarded instead of clobbering the new owner's state. This is only
  sound because `attempts` is never reused for a job (§3).
- **Polling**: fixed interval with jitter (default 1s); poll again immediately if the
  last batch was full. `LISTEN/NOTIFY` wake-ups are a possible later optimisation.
- **Execution**: handlers run on virtual threads bounded by a semaphore
  (`scheduler.worker.concurrency`). The poller blocks until at least one permit is
  free, takes all free permits, claims that many jobs, and returns unused permits; each
  job releases its permit when its handler thread exits.
- **Worker failure modes**: a failed claim query is logged and retried on the next poll.
  If the ack cannot be written (database unreachable), the job stays RUNNING under the
  worker's id and is recovered by the reaper (§7). `Throwable`, not just `Exception`,
  is caught around handlers so an `Error` in one job cannot leak a slot.
- **Failures** go through one fenced statement (`markFailed`) in which the database
  itself decides retry-or-dead from the row being updated, so the budget check cannot
  race with anything (§6). Handlers implement
  `JobHandler { String type(); void handle(JobContext ctx) throws Exception; }` and are
  discovered as Spring beans. A job with an unknown type fails straight to DEAD.

## 6. Retries, backoff, dead-letter queue

- Delay after the *n*-th counted attempt (`n = attempts - attempts_excused`):
  `min(cap, base * 2^(n-1))`, then **full jitter**
  (uniform in `[0, delay]`). Defaults: base 5s, cap 1h. Jitter prevents a burst of
  jobs that failed together (downstream outage) from retrying together.
- Full jitter means a retry can be almost immediate; that is the accepted cost of
  spreading retries over the whole interval rather than clustering them near the ceiling.
- Backoff is implemented by setting `run_at`; there is no separate delay queue.
- The worker computes the delay (config: `scheduler.worker.retry.base-delay`,
  `max-delay`) and passes it to the failure statement; the statement decides the
  outcome: `PENDING` if the failure is retryable and
  `attempts - attempts_excused <= max_retries`, else `DEAD`.
- A handler may throw `NonRetryableException` to go straight to DEAD. A job whose type
  has no registered handler is also non-retryable.
- `last_error` holds the most recent failure and is kept across requeue and later
  success; per-attempt errors live in `job_attempts`.
- **The DLQ is `status = 'DEAD'` in the same table**, not a separate table. Moving to
  the DLQ is then a single atomic guarded update, requeue is another, and the row keeps
  its id and history. The usual argument for a separate table (keeping the hot table
  small) is already handled by partial indexes.
- **Requeue** sets `status = 'PENDING', run_at = now(), attempts_excused = attempts`,
  giving a fresh retry budget while attempt numbering continues (a job that died on
  attempt 4 is next claimed as attempt 5). It does not reset `attempts` to 0 (§3).
- API: `GET /api/v1/dlq` (most recently dead first, keyset on `(finished_at, id)` using
  `jobs_dead_idx`, optional `type` filter), `POST /api/v1/dlq/{id}/requeue` (409 unless DEAD).
- Job responses expose both `attempts` and `attemptsExcused`, so a requeued job showing
  `attempts: 4, maxRetries: 3` is explicable.

## 7. Heartbeats and crash recovery

All timeout comparisons use database `now()`, so worker clock skew is irrelevant.

### Heartbeat
- Each worker registers a row in `workers` at start and updates `last_heartbeat_at`
  every `heartbeat-interval` (default 5s) from a dedicated platform thread, so heartbeats
  never queue behind job execution. (It shares the connection pool; handlers hold no
  connection while running, so the pool is only used for short claim/ack statements.)
- `heartbeat-timeout` (default 30s) must be at least twice the interval; this is
  validated at startup so one late heartbeat cannot get a healthy worker reaped.
- **Why worker-level heartbeats rather than per-job leases**: one row update per worker
  per interval, instead of one per running job. The cost is that a single hung handler
  on a live worker is invisible to the reaper; the execution timeout below covers that.

### Reaper
Runs in every worker process (default every 10s, jittered). One pass is two statements:

1. `ALIVE -> DEAD` for workers whose heartbeat is older than the timeout
   (`FOR UPDATE SKIP LOCKED`, backed by `workers_alive_idx`).
2. Reclaim every RUNNING job whose `locked_by` worker is **not ALIVE** (join via
   `jobs_running_idx`, `FOR UPDATE OF jobs SKIP LOCKED`): to PENDING with
   `run_at = now()`, or DEAD if the budget is spent; the open `job_attempts` row is
   closed as ABANDONED.

- The two steps are deliberately not one transaction. Step 2 sweeps the jobs of *all*
  non-ALIVE workers, not only those step 1 just marked, so the pass is idempotent: if a
  reaper dies between the steps, any reaper's next pass finishes the work.
- Concurrent reapers split rows via `SKIP LOCKED`, and a locked row is re-checked
  against the `WHERE` clause, so each job is reclaimed exactly once.
- An abandoned attempt **is charged** to the retry budget and retried immediately (no
  backoff: the failure was the worker's, and detection already took a timeout). Because
  it is charged, a poison job that kills every worker that touches it reaches the DLQ
  after `max_retries + 1` crashes.
- A zombie's late ack loses to the reaper either way: whichever statement locks the job
  row first wins, and the other finds its `WHERE` guard false.

### Claim guard and self-fencing
- The claim query requires the claiming worker to be ALIVE
  (`EXISTS (SELECT 1 FROM workers WHERE id = :worker AND status = 'ALIVE')`). A worker
  that has been declared dead but does not know yet claims nothing. The narrow race
  where a claim and the DEAD mark interleave is covered by reaper step 2.
- If a heartbeat update matches no row, the worker has been declared dead (it was
  stalled or partitioned, not actually dead). It marks every execution claimed under
  that identity as abandoned, interrupts those handlers, registers a fresh id, and
  continues under it. Results of abandoned executions are never written.

### Execution timeout
Covers a hung handler on a live worker. Each job has `timeout_seconds` (API-settable,
default 300). The owning worker schedules a watchdog per execution. At the deadline it
fails the attempt through the normal fenced failure path
(`job_attempts.outcome = 'TIMED_OUT'`, charged to the budget, then backoff or DEAD) and
interrupts the handler's virtual thread.

- Enforcement is worker-side because only the owner can interrupt the handler. If the
  whole worker is hung, its heartbeat stops and the reaper handles it instead.
- The job is failed **at the deadline**, not when the handler eventually returns, so a
  retry elsewhere is not held hostage by a stuck handler.
- Interruption is cooperative. A handler that ignores interrupts keeps running after its
  job has been failed and possibly retried (another reason handlers must be idempotent).
  Its executor slot is only freed when the thread actually exits, so a stuck handler
  shows up as lost capacity (`Worker.stuckHandlers()`, exported as the
  `jobs.handlers.stuck` gauge in Phase 6) rather than as unbounded thread growth.

### One writer per execution
Four parties can end a running execution: the handler returning, the timeout watchdog,
shutdown release, and self-fencing. Each execution carries an atomic state
(`RUNNING -> FINISHED | TIMED_OUT | RELEASED | ABANDONED`) and only the party that wins
the compare-and-set writes to the database. This is an in-process optimisation and a
source of clean logs; correctness across processes still rests on the SQL fencing.

### Graceful shutdown
On SIGTERM: stop polling, wait up to `shutdown-grace` for in-flight jobs, then release
anything unfinished back to PENDING *without* consuming retry budget, interrupt those
handlers, and mark the worker STOPPED. The heartbeat keeps running until the end so a
draining worker is not reaped. Rolling deploys therefore do not burn retries. Release
keeps `attempts` as is and does `attempts_excused + 1`, closing the `job_attempts` row
as RELEASED; the next claim gets a fresh attempt number (§3). `run_at` is untouched, so
another worker can pick the job up immediately.

## 8. Recurring cron schedules

A schedule is a template that materialises ordinary jobs, which then get everything jobs
get: priority, retries, timeouts, the DLQ. The firing loop runs in every worker process
(`scheduler.schedule.poll-interval`, default 1s). One pass is **one transaction**:

1. `SELECT *, now() FROM schedules WHERE enabled AND next_fire_at <= now()
   ORDER BY next_fire_at LIMIT 100 FOR UPDATE SKIP LOCKED`
2. For each: insert a job with `run_at = next_fire_at` (the tick's nominal time),
   `schedule_id`, and idempotency key `schedule:<id>:<next_fire_at epoch millis>`
   (`ON CONFLICT DO NOTHING`).
3. Set `last_fired_at = now()` and `next_fire_at` to the first cron time strictly after
   the transaction's `now()`.

- **Atomicity is the point of the transaction**: the job insert and the advance commit
  together, so a crash mid-pass can neither lose a tick (advanced, no job) nor repeat one
  (job, not advanced).
- **Two independent guards give one job per tick.** The row lock makes concurrent firers
  take disjoint schedules, and a firer arriving after a commit re-reads the advanced
  `next_fire_at` and finds the schedule not due. Separately, the tick's idempotency key
  admits one job even if the lock were somehow bypassed. Either alone suffices; both
  make it robust to a bug in the other.
- **Misfire policy: fire once, then skip ahead.** If the system was down for an hour, an
  every-minute schedule produces one job, not sixty, because the next fire time is
  computed from now rather than from the missed tick. Catch-up floods are rarely wanted.
  The one job carries the original tick as `run_at`, so its lateness is visible.
- **Next fire time is computed in Java, from the database clock.** The cron library lives
  in the JVM, but "now" comes from the same query that locked the row, so worker clock
  skew still cannot affect scheduling.
- Cron parsing uses Spring's `CronExpression`: six fields
  (`second minute hour day-of-month month day-of-week`) plus macros like `@hourly`.
  Five-field Unix cron is rejected with a message saying so. Expressions are evaluated
  in the schedule's timezone, so "09:00 daily" stays 09:00 local across daylight-saving
  changes.
- Rejected at the API (400): unparseable cron, unknown timezone, and expressions that can
  never fire (e.g. 30 February). A row that becomes unusable anyway (hand-edited) is
  disabled and logged instead of failing the whole batch.
- **Pause / resume** are idempotent. Resume recomputes `next_fire_at` from now, so ticks
  that passed while paused are not made up.
- **Delete** keeps the jobs already created (`ON DELETE SET NULL`).
- Overlap is allowed: if a run is still going when the next tick fires, a second job is
  created. (A "skip if previous still running" option is future work.)
- No update endpoint: to change a schedule, delete and recreate it.

## 9. API and metrics

```
POST   /api/v1/jobs                 submit; 201, or 200 with the existing job on idempotency replay
GET    /api/v1/jobs/{id}            job + `attemptHistory` (one entry per claim, oldest first)
GET    /api/v1/jobs?status=&type=   keyset-paginated list
POST   /api/v1/jobs/{id}/cancel     409 unless PENDING
GET    /api/v1/dlq
POST   /api/v1/dlq/{id}/requeue
POST   /api/v1/schedules            also GET (up to 500, by name), GET /{id}, DELETE /{id}; 409 on duplicate name
POST   /api/v1/schedules/{id}/pause | /resume
GET    /api/v1/workers?status=      200 most recently started, with `runningJobs` per worker
GET    /actuator/prometheus | /actuator/health
```

**Idempotent submit**: `INSERT ... ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING *`;
on no row returned, select and return the existing job. If the replay's body differs
from the stored job, respond 409 rather than silently returning a different job.
- Two concurrent submits of one key: the second `INSERT` blocks on the unique index until
  the first commits, then does nothing, so exactly one caller sees `created`.
- "Differs" is evaluated in SQL against the stored row (type, payload, priority,
  max retries, timeout, run-at), so payloads compare with `jsonb` semantics: key order
  and whitespace do not matter. A request that omits `runAt` matches any stored run-at,
  because its implicit "now" is different on every call. `runAt` is truncated to
  microseconds (the precision of `timestamptz`) before insert and compare.
- The key is a body field and is global, not scoped per job type.

**Listing** uses keyset pagination on `(created_at DESC, id DESC)` with an opaque cursor
(base64 of the last row's position) rather than `OFFSET`: page cost stays constant and
pages do not shift when new jobs are inserted mid-scan.

**Validation limits**: `type` matches `[A-Za-z0-9._-]{1,100}`; `priority` in
[-1000, 1000]; `maxRetries` in [0, 25]; `timeoutSeconds` in [1, 86400];
`idempotencyKey` 1-255 chars. A `runAt` in the past is accepted and means "now".

**Metrics** (Micrometer):
- Counters: `jobs.submitted`, `jobs.claimed`, `jobs.succeeded`, `jobs.failed`,
  `jobs.retried`, `jobs.dead`, `jobs.reclaimed`, `jobs.timed_out`, `jobs.released`,
  `schedules.fired` (tagged by job type)
- Timers: `job.execution.duration`, `job.queue.wait` (claim time − `run_at`), `claim.query.duration`
- Gauges (from a periodically cached `GROUP BY status` query): `jobs.queue.depth{status}`,
  `jobs.oldest.pending.age`, `workers.alive`, `jobs.handlers.stuck`

Queue wait time and oldest-pending age are the metrics that matter operationally: they
say whether the fleet is keeping up, which raw counts do not.

## 10. Build plan

Each phase ends with passing tests and a DESIGN.md update. See the conversation for
approval status.

| # | Phase | Delivers | Key tests |
|---|---|---|---|
| 0 | Scaffold | Maven project, Flyway baseline, Compose with Postgres, Testcontainers base class | context loads; migrations apply to a real Postgres |
| 1 | Submit API | `jobs` table, submit / get / list / cancel, validation, idempotency | idempotent replay returns same job; 20 concurrent submits with one key create one row; validation errors; cancel guards |
| 2 | Claim + execute | `workers` table, claim query, handler registry, executor, success path, `job_attempts` | **8 workers × 1,000 jobs: each executed exactly once**; priority then run_at ordering; future `run_at` not claimed early; stale ack rejected by fencing |
| 3 | Retries + DLQ | backoff with jitter, DEAD state, non-retryable errors, DLQ endpoints | backoff bounds per attempt; job dies after max_retries+1 counted attempts; requeue grants a fresh budget and the next attempt number continues (no `job_attempts` collision); unknown type goes DEAD |
| 4 | Heartbeat + reclaim + timeout | heartbeat thread, reaper, self-fencing, graceful shutdown, per-job execution timeout | kill a worker mid-job, another worker completes it; concurrent reapers reclaim once; zombie worker's late ack ignored; poison job ends in DLQ; shutdown release excuses the attempt and the re-claim gets a new attempt number; hung handler is interrupted, recorded TIMED_OUT, retried, and dies after budget; interrupt-ignoring handler's late ack rejected |
| 5 | Cron | `schedules` table, firing loop, CRUD, pause/resume | 4 firers, one job per tick; misfire fires once; pause stops firing; timezone handling; invalid cron rejected |
| 6 | Metrics + packaging | Micrometer metrics, Dockerfile, Compose with scaled workers, README with a crash-recovery demo and a throughput number | metrics reflect a known workload; Compose smoke test script |

## 11. Known limitations

- **Priority starvation.** The claim query orders strictly by `priority DESC`. If
  higher-priority jobs arrive at least as fast as the fleet can process them, a
  lower-priority job is never claimed, however long it has waited. Nothing bounds its
  wait. It is observable (`jobs.oldest.pending.age`, `job.queue.wait`) but not prevented.
  The future fix is **aging**: effective priority rises with time spent waiting, so
  every job eventually reaches the head. Computing that in the `ORDER BY`
  (`priority + age/k`) would defeat the index-ordered scan of `jobs_claim_idx`, so the
  intended implementation is a stored `effective_priority` column that the index uses
  and a periodic sweep that bumps long-waiting PENDING rows.
- **Timeouts are cooperative** (§7): a handler that ignores interruption cannot be
  killed, only abandoned.
- **At-least-once only** (§1).
- **Finished jobs are never deleted**, so `jobs` and `job_attempts` grow without bound.

## 12. Future work (deliberately out of scope)

- Priority aging (see §11)
- `LISTEN/NOTIFY` to cut polling latency
- Retention: partition or archive finished jobs
- Named queues with per-queue concurrency limits; rate limiting
- "No overlap" policy for schedules
- Auth on the API

## 13. Decision log

| Date | Decision | Reason |
|---|---|---|
| 2026-11-16 | Postgres is the only infrastructure | Transactional queue, one dependency (§1) |
| 2026-11-16 | `JdbcClient` + SQL, no JPA | Locking and guarded updates need exact SQL; ORM state tracking fights that |
| 2026-11-16 | Claim in a short tx, execute outside it | No long-held locks or pinned connections (§5) |
| 2026-11-16 | Attempts incremented at claim | Poison pills terminate (§5) |
| 2026-11-16 | Worker-level heartbeat, not per-job lease | Fewer writes; hung-handler gap accepted for now (§7) |
| 2026-11-16 | No leader election for reaper/cron | Row locks + idempotent SQL make every instance safe (§2) |
| 2026-11-16 | DLQ as a status, not a table | Atomic transitions; partial indexes keep the hot path small (§6) |
| 2026-11-16 | Retry = PENDING with future `run_at` | One "waiting" state (§4) |
| 2026-11-16 | Cron misfires fire once | Avoid catch-up floods (§8) |
| 2026-11-16 | Single artifact, roles by config | Simple build, still scales api and workers independently (§2) |
| 2026-11-16 | Idempotency replay with a different body returns 409 | Returning the old job would hide a client bug (§9) |
| 2026-11-16 | Keyset pagination, not OFFSET | Constant cost, stable under inserts (§9) |
| 2026-11-17 | Claim via a materialized CTE that also inserts the attempt row | `IN (subquery)` can exceed LIMIT under some plans; one statement keeps job and history consistent (§5) |
| 2026-11-17 | Retry-or-dead decided inside the failure UPDATE | The budget check reads the same row version it writes; no read-then-write (§6) |
| 2026-11-17 | Reaper pass is two idempotent statements, not one transaction | Step 2 sweeps all non-ALIVE workers' jobs, so a half-finished pass is completed by the next one (§7) |
| 2026-11-17 | Abandoned (crashed) attempts are charged and retried without backoff | Charging makes poison jobs terminate; backoff would only add to the detection delay (§7) |
| 2026-11-17 | Claim requires the worker to be ALIVE | A worker that does not yet know it was reaped cannot take new work (§7) |
| 2026-11-17 | Timeout fails the job at the deadline; slot freed only when the handler exits | Retry is not blocked by a stuck handler; stuck handlers cost visible capacity, not unbounded threads (§7) |
| 2026-11-17 | One transaction per firing pass (lock, insert job, advance) | A tick can be neither lost nor repeated by a crash mid-pass (§8) |
| 2026-11-17 | Scheduled job's `run_at` is the nominal tick time | Lateness after a misfire shows up in queue-wait metrics (§8) |
| 2026-11-17 | Tests share one Postgres container across Spring contexts | Faster, and fewer container startups to fail; wired by properties because Boot stops container beans with their context |
| 2026-11-16 | `attempts` is monotonic; budget = `attempts - attempts_excused` | Release and requeue must not reuse an attempt number: it is the `job_attempts` key and the fencing token (§3) |
| 2026-11-16 | Per-job execution timeout, enforced by the owning worker (Phase 4) | Worker heartbeats cannot see one hung handler; only the owner can interrupt it (§7) |
| 2026-11-16 | Strict priority, starvation accepted and documented | Aging needs a stored effective priority to keep the claim index usable; deferred (§11) |
