-- Worker registry and per-attempt execution history (DESIGN.md §3).
CREATE TABLE workers (
    id                uuid PRIMARY KEY,
    hostname          text        NOT NULL,
    started_at        timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    status            text        NOT NULL DEFAULT 'ALIVE'
                      CHECK (status IN ('ALIVE','STOPPED','DEAD'))
);

ALTER TABLE jobs ADD CONSTRAINT jobs_locked_by_fkey FOREIGN KEY (locked_by) REFERENCES workers(id);

CREATE TABLE job_attempts (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     int         NOT NULL,
    worker_id   uuid        NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    outcome     text CHECK (outcome IN ('SUCCEEDED','FAILED','TIMED_OUT','ABANDONED','RELEASED')),
    error       text,
    -- Also the lookup index for a job's history and for closing an attempt.
    UNIQUE (job_id, attempt)
);
