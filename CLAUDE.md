# Task Scheduler

A distributed task scheduler (a mini Celery/Quartz) backed by PostgreSQL. Resume project
targeting backend roles: the point is to demonstrate correct concurrency, failure handling,
and clear design reasoning, not feature breadth.

## Goals

- Submit jobs over REST: type, payload, priority, run-at time, max retries, idempotency key.
- Multiple workers claim jobs atomically with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Retries with exponential backoff and jitter; dead-letter queue when retries are exhausted.
- Worker heartbeats; jobs held by crashed workers are reclaimed.
- Recurring cron schedules that fire exactly once per tick across all instances.
- Metrics endpoint (Micrometer / Prometheus).
- Delivery guarantee: **at-least-once**. Handlers must be idempotent.

Non-goals: a message broker, multi-tenant auth, a UI, workflow/DAG dependencies, sharding.

## Stack

- Java 21, Spring Boot, Maven (wrapper checked in)
- PostgreSQL 16, Flyway migrations
- Spring `JdbcClient` with hand-written SQL (no JPA: the SQL is the interesting part)
- JUnit 5, AssertJ, Testcontainers (real Postgres, never H2), Awaitility
- Docker Compose: postgres + 1 api + N workers from one image

## Documents

- `DESIGN.md` is the source of truth for architecture, schema, state machine, and the
  reasoning behind decisions. **Update it in the same change as any code that alters
  the schema, the state machine, a guarantee, or a decision recorded there.** New
  decisions get an entry in its decision log.
- Work follows the phased plan in `DESIGN.md` §10. Do not start a phase before the
  previous one's tests pass and the user has approved moving on.

## Conventions

### Code
- Base package `dev.scheduler`, organised by feature: `job`, `worker`, `schedule`,
  `metrics`, `common`. No `controller/service/repository` top-level layers.
- Records for DTOs and row types. Constructor injection only. No Lombok.
- All state transitions go through SQL in `JobRepository`, and every transition is a
  guarded `UPDATE ... WHERE status = <expected>` (plus `locked_by` / `attempts` fencing
  for worker-owned transitions). Check the affected row count; never read-then-write.
- Time comes from the database (`now()`) for anything scheduling-related, so worker
  clock skew cannot affect correctness. Java-side time uses an injected `Clock`.
- Never hold a DB transaction open while a job handler runs.
- Timestamps are `timestamptz` / `Instant`, UTC everywhere.
- Configuration lives under the `scheduler.*` prefix as `@ConfigurationProperties` records.

### Database
- Schema changes only via new Flyway files `src/main/resources/db/migration/V<n>__<desc>.sql`.
  Never edit an applied migration.
- Every query on the hot path (claim, reap, schedule fire) must be backed by an index;
  note the index in DESIGN.md.

### API
- JSON, prefix `/api/v1`. Errors use RFC 9457 `ProblemDetail`.
- Validate at the boundary with Bean Validation.

### Tests
- Every phase ships with tests. Integration tests use Testcontainers Postgres.
- Concurrency claims must be proven by a test that runs real concurrent workers
  (e.g. N workers, M jobs, assert each job executed exactly once).
- No `Thread.sleep` in tests: use Awaitility, and make intervals configurable so tests
  run with millisecond timings.

### Commands
- JDK 21 is the Homebrew keg: `export JAVA_HOME=/opt/homebrew/opt/openjdk@21` before `./mvnw`
- `./mvnw verify` runs all tests (requires Docker running)
- `./mvnw test -Dtest=ClassName` runs a single test class
- `docker compose up --build --scale worker=3` runs the full stack

### Git
- Small commits per logical step, imperative subject line. Commit only when asked.
