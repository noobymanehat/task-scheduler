package dev.scheduler.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

/**
 * All SQL touching {@code jobs}. Every state transition is a single guarded UPDATE whose
 * WHERE clause names the expected current status; callers learn the outcome from whether a
 * row came back, never by reading first and writing second.
 */
@Repository
public class JobRepository {

    private final JdbcClient jdbc;
    private final RowMapper<Job> jobMapper;

    public JobRepository(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.jobMapper = (rs, rowNum) -> new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                json.readTree(rs.getString("payload")),
                rs.getInt("priority"),
                JobStatus.valueOf(rs.getString("status")),
                instant(rs, "run_at"),
                rs.getInt("attempts"),
                rs.getInt("attempts_excused"),
                rs.getInt("max_retries"),
                rs.getInt("timeout_seconds"),
                rs.getString("idempotency_key"),
                rs.getObject("locked_by", UUID.class),
                instant(rs, "locked_at"),
                rs.getString("last_error"),
                rs.getObject("schedule_id", UUID.class),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "finished_at"));
    }

    /**
     * Inserts a job. Returns empty when a job with the same idempotency key already exists.
     * If a concurrent insert of the same key is in flight, ON CONFLICT waits for it to commit
     * or roll back, so exactly one caller gets a row back.
     */
    public Optional<Job> insert(NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (type, payload, priority, run_at, max_retries, timeout_seconds, idempotency_key)
                        VALUES (:type, CAST(:payload AS jsonb), :priority,
                                COALESCE(CAST(:runAt AS timestamptz), now()),
                                :maxRetries, :timeoutSeconds, :idempotencyKey)
                        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                        RETURNING *
                        """)
                .params(submitParams(job))
                .query(jobMapper)
                .optional();
    }

    /**
     * Inserts the job for one tick of a schedule, with run_at set to the tick's nominal fire
     * time. Returns false if a job with this tick's key already exists.
     */
    public boolean insertForSchedule(
            UUID scheduleId,
            String type,
            String payloadJson,
            int priority,
            Instant runAt,
            int maxRetries,
            int timeoutSeconds,
            String tickKey) {
        return jdbc.sql("""
                        INSERT INTO jobs (type, payload, priority, run_at, max_retries, timeout_seconds,
                                          idempotency_key, schedule_id)
                        VALUES (:type, CAST(:payload AS jsonb), :priority, :runAt, :maxRetries, :timeoutSeconds,
                                :tickKey, :scheduleId)
                        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                        """)
                        .param("type", type)
                        .param("payload", payloadJson)
                        .param("priority", priority)
                        .param("runAt", timestamptz(runAt))
                        .param("maxRetries", maxRetries)
                        .param("timeoutSeconds", timeoutSeconds)
                        .param("tickKey", tickKey)
                        .param("scheduleId", scheduleId)
                        .update()
                == 1;
    }

    /**
     * Finds the job holding an idempotency key and reports whether it was created by an
     * equivalent request. The comparison runs in SQL so JSON is compared with jsonb
     * semantics (key order and whitespace are irrelevant). A request that left run-at
     * unset matches any stored run-at, since its "now" differs on every call.
     */
    public Optional<IdempotentMatch> findByIdempotencyKey(NewJob request) {
        return jdbc.sql("""
                        SELECT *,
                               (type = :type
                                AND payload = CAST(:payload AS jsonb)
                                AND priority = :priority
                                AND max_retries = :maxRetries
                                AND timeout_seconds = :timeoutSeconds
                                AND (CAST(:runAt AS timestamptz) IS NULL
                                     OR run_at = CAST(:runAt AS timestamptz))) AS same_request
                        FROM jobs
                        WHERE idempotency_key = :idempotencyKey
                        """)
                .params(submitParams(request))
                .query((rs, n) -> new IdempotentMatch(jobMapper.mapRow(rs, n), rs.getBoolean("same_request")))
                .optional();
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT * FROM jobs WHERE id = :id").param("id", id).query(jobMapper).optional();
    }

    /**
     * Newest-first keyset page. {@code after} is the (created_at, id) of the last row of the
     * previous page; keyset pagination stays stable and index-backed while rows are inserted.
     */
    public List<Job> list(JobStatus status, String type, Instant afterCreatedAt, UUID afterId, int limit) {
        var sql = new StringBuilder("SELECT * FROM jobs WHERE true");
        Map<String, Object> params = new HashMap<>();
        if (status != null) {
            sql.append(" AND status = :status");
            params.put("status", status.name());
        }
        if (type != null) {
            sql.append(" AND type = :type");
            params.put("type", type);
        }
        if (afterCreatedAt != null) {
            sql.append(" AND (created_at, id) < (:afterCreatedAt, :afterId)");
            params.put("afterCreatedAt", timestamptz(afterCreatedAt));
            params.put("afterId", afterId);
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :limit");
        params.put("limit", limit);
        return jdbc.sql(sql.toString()).params(params).query(jobMapper).list();
    }

    /** PENDING -> CANCELLED. Empty if the job does not exist or is not PENDING. */
    public Optional<Job> cancel(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'CANCELLED', finished_at = now(), updated_at = now()
                        WHERE id = :id AND status = 'PENDING'
                        RETURNING *
                        """)
                .param("id", id)
                .query(jobMapper)
                .optional();
    }

    /**
     * Atomically claims up to {@code limit} due jobs for a worker: PENDING -> RUNNING,
     * attempts + 1, and one job_attempts row per job, all in a single statement.
     *
     * <p>FOR UPDATE SKIP LOCKED makes concurrent claimers pass over each other's rows instead
     * of blocking on them. The pick is a MATERIALIZED CTE rather than {@code id IN (subquery)}
     * so the locking subquery runs exactly once and LIMIT is honoured whatever plan is chosen.
     *
     * <p>A worker that is not ALIVE (declared dead by a reaper, or stopped) claims nothing.
     */
    public List<Job> claim(UUID workerId, int limit) {
        return jdbc.sql("""
                        WITH picked AS MATERIALIZED (
                            SELECT id FROM jobs
                            WHERE status = 'PENDING' AND run_at <= now()
                              AND EXISTS (SELECT 1 FROM workers w WHERE w.id = :worker AND w.status = 'ALIVE')
                            ORDER BY priority DESC, run_at, id
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        ), claimed AS (
                            UPDATE jobs j
                            SET status = 'RUNNING', locked_by = :worker, locked_at = now(),
                                attempts = j.attempts + 1, updated_at = now()
                            FROM picked
                            WHERE j.id = picked.id
                            RETURNING j.*
                        ), recorded AS (
                            INSERT INTO job_attempts (job_id, attempt, worker_id, started_at)
                            SELECT id, attempts, :worker, now() FROM claimed
                        )
                        SELECT * FROM claimed ORDER BY priority DESC, run_at, id
                        """)
                .param("worker", workerId)
                .param("limit", limit)
                .query(jobMapper)
                .list();
    }

    /**
     * RUNNING -> SUCCEEDED, only if {@code workerId} still owns the job on this attempt.
     * Returns false for a stale ack (the job was reclaimed, or already finished).
     */
    public boolean markSucceeded(UUID jobId, UUID workerId, int attempt) {
        return jdbc.sql("""
                        WITH done AS (
                            UPDATE jobs
                            SET status = 'SUCCEEDED', finished_at = now(), updated_at = now(),
                                locked_by = NULL, locked_at = NULL
                            WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker AND attempts = :attempt
                            RETURNING id, attempts
                        ), closed AS (
                            UPDATE job_attempts a
                            SET finished_at = now(), outcome = 'SUCCEEDED'
                            FROM done
                            WHERE a.job_id = done.id AND a.attempt = done.attempts
                        )
                        SELECT count(*) FROM done
                        """)
                        .param("id", jobId)
                        .param("worker", workerId)
                        .param("attempt", attempt)
                        .query(Long.class)
                        .single()
                == 1;
    }

    /**
     * Records a failed attempt, with the same fencing as {@link #markSucceeded}. The database
     * decides the next state from the row it is updating: back to PENDING with
     * {@code run_at = now() + retryDelay} if the failure is retryable and budget remains
     * ({@code attempts - attempts_excused <= max_retries}), otherwise DEAD.
     *
     * @return the job's new status, or empty for a stale ack
     */
    public Optional<JobStatus> markFailed(
            UUID jobId, UUID workerId, int attempt, String error, boolean retryable, Duration retryDelay) {
        return fail(jobId, workerId, attempt, error, retryable, retryDelay, "FAILED");
    }

    /** As {@link #markFailed} (retryable), recorded as a TIMED_OUT attempt. */
    public Optional<JobStatus> markTimedOut(UUID jobId, UUID workerId, int attempt, String error, Duration retryDelay) {
        return fail(jobId, workerId, attempt, error, true, retryDelay, "TIMED_OUT");
    }

    private Optional<JobStatus> fail(
            UUID jobId,
            UUID workerId,
            int attempt,
            String error,
            boolean retryable,
            Duration retryDelay,
            String outcome) {
        return jdbc.sql("""
                        WITH target AS (
                            SELECT id, (:retryable AND attempts - attempts_excused <= max_retries) AS retry
                            FROM jobs
                            WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker AND attempts = :attempt
                            FOR UPDATE
                        ), done AS (
                            UPDATE jobs j
                            SET status = CASE WHEN target.retry THEN 'PENDING' ELSE 'DEAD' END,
                                run_at = CASE WHEN target.retry
                                              THEN now() + :delayMillis * interval '1 millisecond'
                                              ELSE j.run_at END,
                                finished_at = CASE WHEN target.retry THEN NULL ELSE now() END,
                                locked_by = NULL, locked_at = NULL,
                                last_error = :error, updated_at = now()
                            FROM target
                            WHERE j.id = target.id
                            RETURNING j.id, j.attempts, j.status
                        ), closed AS (
                            UPDATE job_attempts a
                            SET finished_at = now(), outcome = :outcome, error = :error
                            FROM done
                            WHERE a.job_id = done.id AND a.attempt = done.attempts
                        )
                        SELECT status FROM done
                        """)
                .param("id", jobId)
                .param("worker", workerId)
                .param("attempt", attempt)
                .param("error", error)
                .param("retryable", retryable)
                .param("delayMillis", retryDelay.toMillis())
                .param("outcome", outcome)
                .query(String.class)
                .optional()
                .map(JobStatus::valueOf);
    }

    /**
     * RUNNING -> PENDING at graceful shutdown, fenced like an ack. The attempt is excused
     * ({@code attempts_excused + 1}) so it does not count against the retry budget, while
     * {@code attempts} stays put so the next claim gets a new attempt number. run_at is left
     * alone: the job is immediately claimable by another worker.
     */
    public boolean release(UUID jobId, UUID workerId, int attempt) {
        return jdbc.sql("""
                        WITH done AS (
                            UPDATE jobs
                            SET status = 'PENDING', attempts_excused = attempts_excused + 1,
                                locked_by = NULL, locked_at = NULL, updated_at = now()
                            WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker AND attempts = :attempt
                            RETURNING id, attempts
                        ), closed AS (
                            UPDATE job_attempts a
                            SET finished_at = now(), outcome = 'RELEASED'
                            FROM done
                            WHERE a.job_id = done.id AND a.attempt = done.attempts
                        )
                        SELECT count(*) FROM done
                        """)
                        .param("id", jobId)
                        .param("worker", workerId)
                        .param("attempt", attempt)
                        .query(Long.class)
                        .single()
                == 1;
    }

    /**
     * Takes back every RUNNING job held by a worker that is no longer ALIVE. The attempt
     * counts against the budget (a job that keeps killing its worker must eventually stop),
     * so each job goes to PENDING, runnable now, or to DEAD if its budget is spent.
     *
     * <p>FOR UPDATE SKIP LOCKED means concurrent reapers split the rows between them, and a
     * row is re-checked after locking, so each job is reclaimed exactly once.
     *
     * @return the new status of each reclaimed job
     */
    public List<JobStatus> reclaimFromDeadWorkers() {
        return jdbc.sql("""
                        WITH orphaned AS (
                            SELECT j.id, j.locked_by AS worker_id,
                                   (j.attempts - j.attempts_excused <= j.max_retries) AS retry
                            FROM jobs j
                            JOIN workers w ON w.id = j.locked_by
                            WHERE j.status = 'RUNNING' AND w.status <> 'ALIVE'
                            FOR UPDATE OF j SKIP LOCKED
                        ), done AS (
                            UPDATE jobs j
                            SET status = CASE WHEN o.retry THEN 'PENDING' ELSE 'DEAD' END,
                                run_at = CASE WHEN o.retry THEN now() ELSE j.run_at END,
                                finished_at = CASE WHEN o.retry THEN NULL ELSE now() END,
                                last_error = 'Worker ' || CAST(o.worker_id AS text)
                                             || ' stopped heartbeating; attempt abandoned',
                                locked_by = NULL, locked_at = NULL, updated_at = now()
                            FROM orphaned o
                            WHERE j.id = o.id
                            RETURNING j.id, j.attempts, j.status, j.last_error
                        ), closed AS (
                            UPDATE job_attempts a
                            SET finished_at = now(), outcome = 'ABANDONED', error = done.last_error
                            FROM done
                            WHERE a.job_id = done.id AND a.attempt = done.attempts
                        )
                        SELECT status FROM done
                        """)
                .query(String.class)
                .list()
                .stream()
                .map(JobStatus::valueOf)
                .toList();
    }

    /** Dead jobs, most recently dead first, keyset-paginated on (finished_at, id). */
    public List<Job> listDead(String type, Instant afterFinishedAt, UUID afterId, int limit) {
        var sql = new StringBuilder("SELECT * FROM jobs WHERE status = 'DEAD'");
        Map<String, Object> params = new HashMap<>();
        if (type != null) {
            sql.append(" AND type = :type");
            params.put("type", type);
        }
        if (afterFinishedAt != null) {
            sql.append(" AND (finished_at, id) < (:afterFinishedAt, :afterId)");
            params.put("afterFinishedAt", timestamptz(afterFinishedAt));
            params.put("afterId", afterId);
        }
        sql.append(" ORDER BY finished_at DESC, id DESC LIMIT :limit");
        params.put("limit", limit);
        return jdbc.sql(sql.toString()).params(params).query(jobMapper).list();
    }

    /**
     * DEAD -> PENDING, runnable now, with a fresh retry budget. The budget is restored by
     * excusing every attempt so far; {@code attempts} itself is never reset, so the next
     * claim gets a new attempt number (DESIGN.md §3).
     */
    public Optional<Job> requeue(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'PENDING', run_at = now(), attempts_excused = attempts,
                            finished_at = NULL, updated_at = now()
                        WHERE id = :id AND status = 'DEAD'
                        RETURNING *
                        """)
                .param("id", id)
                .query(jobMapper)
                .optional();
    }

    public List<JobAttempt> findAttempts(UUID jobId) {
        return jdbc.sql("SELECT * FROM job_attempts WHERE job_id = :id ORDER BY attempt")
                .param("id", jobId)
                .query((rs, n) -> new JobAttempt(
                        rs.getInt("attempt"),
                        rs.getObject("worker_id", UUID.class),
                        instant(rs, "started_at"),
                        instant(rs, "finished_at"),
                        rs.getString("outcome"),
                        rs.getString("error")))
                .list();
    }

    public record IdempotentMatch(Job job, boolean sameRequest) {}

    private static Map<String, Object> submitParams(NewJob job) {
        Map<String, Object> params = new HashMap<>();
        params.put("type", job.type());
        params.put("payload", job.payloadJson());
        params.put("priority", job.priority());
        params.put("runAt", job.runAt() == null ? null : timestamptz(job.runAt()));
        params.put("maxRetries", job.maxRetries());
        params.put("timeoutSeconds", job.timeoutSeconds());
        params.put("idempotencyKey", job.idempotencyKey());
        return params;
    }

    // The Postgres driver maps timestamptz to OffsetDateTime, not Instant.
    private static OffsetDateTime timestamptz(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
