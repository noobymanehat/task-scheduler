package dev.scheduler.job;

import java.sql.ResultSet;
import java.sql.SQLException;
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
