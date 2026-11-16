-- Recurring cron schedules (DESIGN.md §8). A schedule is a template that materialises
-- ordinary jobs; deleting one keeps the jobs it already produced.
CREATE TABLE schedules (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL UNIQUE,
    cron            text        NOT NULL,
    timezone        text        NOT NULL DEFAULT 'UTC',
    job_type        text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}',
    priority        int         NOT NULL DEFAULT 0,
    max_retries     int         NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    timeout_seconds int         NOT NULL DEFAULT 300 CHECK (timeout_seconds > 0),
    enabled         boolean     NOT NULL DEFAULT true,
    next_fire_at    timestamptz NOT NULL,
    last_fired_at   timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Firing loop: due, enabled schedules in fire-time order.
CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE enabled;

ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_id_fkey
    FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE SET NULL;
-- Lets the ON DELETE SET NULL find a schedule's jobs without scanning the table.
CREATE INDEX jobs_schedule_idx ON jobs (schedule_id) WHERE schedule_id IS NOT NULL;
