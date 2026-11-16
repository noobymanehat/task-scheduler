-- Jobs table (DESIGN.md §3). The foreign keys locked_by -> workers and
-- schedule_id -> schedules are added by the migrations that create those tables.
CREATE TABLE jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    priority         int         NOT NULL DEFAULT 0,
    status           text        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','DEAD','CANCELLED')),
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    attempts_excused int         NOT NULL DEFAULT 0 CHECK (attempts_excused >= 0),
    max_retries      int         NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    timeout_seconds  int         NOT NULL DEFAULT 300 CHECK (timeout_seconds > 0),
    idempotency_key  text,
    locked_by        uuid,
    locked_at        timestamptz,
    last_error       text,
    schedule_id      uuid,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CHECK (attempts_excused <= attempts)
);

-- Claim query: index-ordered scan over only the claimable rows.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at, id) WHERE status = 'PENDING';
-- Reaper: jobs held by a given worker.
CREATE INDEX jobs_running_idx ON jobs (locked_by) WHERE status = 'RUNNING';
-- Idempotent submission.
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
-- DLQ listing.
CREATE INDEX jobs_dead_idx ON jobs (finished_at DESC) WHERE status = 'DEAD';
-- Keyset pagination for GET /api/v1/jobs.
CREATE INDEX jobs_list_idx ON jobs (created_at DESC, id DESC);
