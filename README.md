# Task Scheduler

A distributed job scheduler (a small Celery / Quartz) where **PostgreSQL is the only
infrastructure**: it is the queue, the lock manager and the source of truth. Clients submit
jobs over REST; any number of identical workers claim and run them.

- **Atomic claiming** with `SELECT ... FOR UPDATE SKIP LOCKED`: N workers never run the same
  claim twice and never block each other.
- **Retries** with exponential backoff and full jitter, then a **dead-letter queue**.
- **Crash recovery**: workers heartbeat; jobs held by a dead worker are reclaimed and rerun.
- **Graceful shutdown**: a stopping worker drains, then hands unfinished jobs back without
  spending their retry budget.
- **Cron schedules** that fire exactly one job per tick, however many instances are running.
- **Per-job timeouts**, priorities, delayed jobs, idempotency keys, Prometheus metrics.

The delivery guarantee is **at-least-once**: a job can run more than once (a worker may crash
after doing the work but before recording it), so handlers must be idempotent.

[`DESIGN.md`](DESIGN.md) explains the schema, the state machine, every SQL statement on the
hot path, and why each decision was made.

## Quickstart

Requires Docker.

```bash
docker compose up --build        # postgres + 1 api + 3 workers
```

Submit a job:

```bash
curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "type": "demo.sleep",
  "payload": {"millis": 500},
  "priority": 5,
  "maxRetries": 3,
  "idempotencyKey": "readme-1"
}'
```

```json
{"id":"38423c2b-0681-469a-90c2-5a9125a3da9e","type":"demo.sleep","payload":{"millis":500},
 "priority":5,"status":"PENDING","runAt":"2026-11-17T03:07:38.593705Z","attempts":0,
 "attemptsExcused":0,"maxRetries":3,"timeoutSeconds":300,"idempotencyKey":"readme-1", ...}
```

Only `type` is required. Sending the same `idempotencyKey` again returns the existing job
(200 instead of 201); sending it with a different body returns 409.

```bash
curl -s localhost:8080/api/v1/jobs/<id>            # status + one history entry per attempt
curl -s 'localhost:8080/api/v1/jobs?status=SUCCEEDED'
curl -s localhost:8080/api/v1/workers
```

Three demo job types are built in:

| Type | Behaviour |
|---|---|
| `demo.noop` | Succeeds immediately |
| `demo.sleep` | Sleeps for payload `millis` (default 1000) |
| `demo.flaky` | Fails until the attempt number reaches payload `succeedOnAttempt`; without it, fails every time and ends in the DLQ |

## Demo: kill a worker mid-job

Start a 20-second job and find the container running it (a worker's hostname is its
container id):

```bash
curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
  -d '{"type": "demo.sleep", "payload": {"millis": 20000}, "idempotencyKey": "crash-demo"}'

curl -s 'localhost:8080/api/v1/workers?status=ALIVE'    # the one with "runningJobs": 1
docker kill <hostname>                                  # SIGKILL: no chance to clean up
```

Within about 13 seconds (10 s heartbeat timeout + 3 s reaper interval in `compose.yaml`)
another worker declares it dead, takes the job back and reruns it. The job's history shows
both attempts:

```bash
curl -s 'localhost:8080/api/v1/jobs?type=demo.sleep'    # find the id
curl -s localhost:8080/api/v1/jobs/<id>                 # attemptHistory: ABANDONED, then SUCCEEDED
```

Use `docker stop <hostname>` instead to see graceful shutdown: the worker waits up to 30 s
for the job, then releases it. The first attempt is recorded as `RELEASED`, `attemptsExcused`
becomes 1, and the worker ends as `STOPPED` rather than `DEAD`.

`scripts/smoke-test.sh` runs all of this unattended and checks the results: 200 jobs across
3 workers with exactly one attempt each, a flaky job that succeeds on its third attempt,
`docker kill` recovery, `docker stop` release, and the metrics endpoint.

## More of the API

```bash
# Retries and the dead-letter queue
curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
  -d '{"type": "demo.flaky", "maxRetries": 2}'          # fails 3 times, then DEAD
curl -s localhost:8080/api/v1/dlq
curl -s -X POST localhost:8080/api/v1/dlq/<id>/requeue  # fresh retry budget

# Delayed job; cancel it while it is still pending
curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
  -d '{"type": "demo.noop", "runAt": "2030-01-01T00:00:00Z"}'
curl -s -X POST localhost:8080/api/v1/jobs/<id>/cancel

# A cron schedule: six fields, seconds first, evaluated in the given timezone
curl -s -X POST localhost:8080/api/v1/schedules -H 'Content-Type: application/json' \
  -d '{"name": "every-10s", "cron": "*/10 * * * * *", "jobType": "demo.noop", "timezone": "UTC"}'
curl -s -X POST localhost:8080/api/v1/schedules/<id>/pause
curl -s -X POST localhost:8080/api/v1/schedules/<id>/resume
curl -s -X DELETE localhost:8080/api/v1/schedules/<id>
```

Errors are RFC 9457 problem documents:

```json
{"title":"Bad Request","status":400,"detail":"Invalid request content.",
 "instance":"/api/v1/jobs","errors":{"type":"may contain only letters, digits, '.', '_' and '-'"}}
```

The full endpoint list is in [DESIGN.md §9](DESIGN.md#9-api-and-metrics).

## Metrics

Every process serves Prometheus metrics at `/actuator/prometheus`.

```bash
curl -s localhost:8080/actuator/prometheus | grep -E '^(jobs|workers)_'
```

| Metric | Meaning |
|---|---|
| `jobs_queue_depth{state}` | Jobs that are `ready`, `scheduled` (future run-at or backing off), `running`, `dead` |
| `jobs_oldest_pending_age_seconds` | How long the oldest ready job has waited. The "are we keeping up" signal |
| `workers_alive` | Workers with a current heartbeat |
| `job_queue_wait_seconds{type}` | Time from a job becoming due to being claimed |
| `job_execution_duration_seconds{type,outcome}` | Time spent in the handler |
| `claim_query_duration_seconds` | Time per claim query |
| `jobs_{submitted,claimed,succeeded,failed,retried,dead,timed_out,released}_total{type}` | Lifecycle counters |
| `jobs_reclaimed_total{result}` | Jobs taken back from dead workers |
| `schedules_fired_total` | Jobs created by cron schedules |
| `jobs_handlers_stuck` | Timed-out handlers that ignored interruption and still hold a slot |

Counters and timers are per process: the API reports submissions, each worker reports its own
claims and executions. The queue gauges come from the database, so any instance reports the
same values. Only the API's port is published by `compose.yaml`, and the stack does not
include a Prometheus server; workers listen on 8080 inside the Compose network.

## Throughput

About **11,500 no-op jobs/s with 3 workers** (30 concurrent slots) on one laptop:

| Run | Jobs | Elapsed | Throughput |
|---|---|---|---|
| 1 (cold JVMs) | 20,000 | 2.92 s | 6,852 jobs/s |
| 2 | 50,000 | 4.31 s | 11,595 jobs/s |
| 3 | 50,000 | 4.33 s | 11,543 jobs/s |

Measured with `scripts/benchmark.sh` on an 8-core Apple M3 with 16 GB, everything (Postgres,
API, workers) in Docker Desktop on that one machine. The script inserts the jobs with one SQL
statement and times first claim to last ack on the database clock, so this is the scheduler's
own overhead per job (one claim statement per batch, one ack statement per job) with an empty
handler. It is not a measurement of submission over HTTP, and real handlers will dominate it.

```bash
docker compose up -d --build
scripts/benchmark.sh 50000
```

## Writing a handler

Implement `JobHandler` and expose it as a Spring bean; the worker finds it by `type()`.

```java
@Component
class SendWelcomeEmail implements JobHandler {

    @Override
    public String type() {
        return "email.welcome";
    }

    @Override
    public void handle(JobContext context) throws Exception {
        String to = context.payload().path("to").asText();
        // Must be idempotent: this can run more than once for the same job.
        // Throw to retry with backoff; throw NonRetryableException to go straight to the DLQ.
    }
}
```

A handler that exceeds the job's `timeoutSeconds` is interrupted and the attempt is recorded
as `TIMED_OUT`, so blocking calls should respond to interruption.

## Configuration

Set in `application.yml` or as environment variables (`SCHEDULER_WORKER_CONCURRENCY=20`).

| Property | Default | Meaning |
|---|---|---|
| `scheduler.api.enabled` | `true` | Serve the REST API |
| `scheduler.worker.enabled` | `true` | Run the worker, reaper and cron firer |
| `scheduler.worker.concurrency` | `10` | Jobs executing at once per worker |
| `scheduler.worker.poll-interval` | `1s` | Pause between claims when the queue is not full |
| `scheduler.worker.shutdown-grace` | `30s` | How long shutdown waits for running jobs before releasing them |
| `scheduler.worker.heartbeat-interval` | `5s` | How often a worker heartbeats |
| `scheduler.worker.heartbeat-timeout` | `30s` | Heartbeat age at which a worker is declared dead |
| `scheduler.worker.reaper-interval` | `10s` | How often dead workers are looked for |
| `scheduler.worker.retry.base-delay` / `max-delay` | `5s` / `1h` | Backoff: `min(max, base * 2^(n-1))`, full jitter |
| `scheduler.schedule.poll-interval` | `1s` | How often due cron schedules are fired |
| `scheduler.metrics.refresh-interval` | `5s` | How long queue gauges are cached |
| `scheduler.demo-handlers.enabled` | `true` | Register the `demo.*` handlers |

If you raise `shutdown-grace`, raise `spring.lifecycle.timeout-per-shutdown-phase` (45 s) and
the container's stop timeout (`stop_grace_period`, 40 s in `compose.yaml`) to stay above it.

## Development

Requires JDK 21 and a running Docker daemon (tests use Testcontainers with a real Postgres).

```bash
./mvnw verify                          # all tests
./mvnw test -Dtest=CrashRecoveryTest
scripts/smoke-test.sh                  # end-to-end against the Compose stack

docker compose up postgres             # database only, then run the app from an IDE
docker compose up --build --scale worker=5
```

The concurrency claims are tested with real concurrent workers against a real database, for
example: 8 workers and 1,000 jobs with each job executed exactly once; 4 cron firers producing
one job per tick; concurrent reapers reclaiming a dead worker's job once; a stalled worker's
late result being rejected by fencing.

Stack: Java 21, Spring Boot 4, PostgreSQL 16, Flyway, `JdbcClient` with hand-written SQL,
Micrometer, JUnit 5, Testcontainers, Awaitility.

## Limitations

- At-least-once only; handlers must be idempotent.
- Priority is strict, so a steady stream of high-priority jobs can starve lower ones.
- Timeouts are cooperative: a handler that ignores interruption cannot be killed.
- Finished jobs are never deleted.
- No authentication.

Details and intended fixes are in [DESIGN.md §11-12](DESIGN.md#11-known-limitations).
