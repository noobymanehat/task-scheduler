package dev.scheduler.schedule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class ScheduleRepository {

    private final JdbcClient jdbc;
    private final RowMapper<Schedule> mapper;

    public ScheduleRepository(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.mapper = (rs, n) -> new Schedule(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("cron"),
                rs.getString("timezone"),
                rs.getString("job_type"),
                json.readTree(rs.getString("payload")),
                rs.getInt("priority"),
                rs.getInt("max_retries"),
                rs.getInt("timeout_seconds"),
                rs.getBoolean("enabled"),
                instant(rs, "next_fire_at"),
                instant(rs, "last_fired_at"),
                instant(rs, "created_at"));
    }

    /** A schedule that is due, together with the database's idea of the current time. */
    public record Due(Schedule schedule, Instant now) {}

    /** The database clock. Inside a transaction this is the transaction's start time. */
    public Instant now() {
        return jdbc.sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
    }

    /** Throws {@code DuplicateKeyException} if the name is taken. */
    public Schedule insert(
            String name,
            String cron,
            String timezone,
            String jobType,
            String payloadJson,
            int priority,
            int maxRetries,
            int timeoutSeconds,
            boolean enabled,
            Instant nextFireAt) {
        return jdbc.sql("""
                        INSERT INTO schedules (name, cron, timezone, job_type, payload, priority,
                                               max_retries, timeout_seconds, enabled, next_fire_at)
                        VALUES (:name, :cron, :timezone, :jobType, CAST(:payload AS jsonb), :priority,
                                :maxRetries, :timeoutSeconds, :enabled, :nextFireAt)
                        RETURNING *
                        """)
                .param("name", name)
                .param("cron", cron)
                .param("timezone", timezone)
                .param("jobType", jobType)
                .param("payload", payloadJson)
                .param("priority", priority)
                .param("maxRetries", maxRetries)
                .param("timeoutSeconds", timeoutSeconds)
                .param("enabled", enabled)
                .param("nextFireAt", timestamptz(nextFireAt))
                .query(mapper)
                .single();
    }

    public Optional<Schedule> findById(UUID id) {
        return jdbc.sql("SELECT * FROM schedules WHERE id = :id").param("id", id).query(mapper).optional();
    }

    public List<Schedule> list(int limit) {
        return jdbc.sql("SELECT * FROM schedules ORDER BY name LIMIT :limit")
                .param("limit", limit)
                .query(mapper)
                .list();
    }

    public boolean delete(UUID id) {
        return jdbc.sql("DELETE FROM schedules WHERE id = :id").param("id", id).update() == 1;
    }

    /** Idempotent: pausing a paused schedule changes nothing. Empty if it does not exist. */
    public Optional<Schedule> pause(UUID id) {
        return jdbc.sql("UPDATE schedules SET enabled = false WHERE id = :id RETURNING *")
                .param("id", id)
                .query(mapper)
                .optional();
    }

    /** Locks one schedule row for the rest of the transaction. */
    public Optional<Due> lock(UUID id) {
        return jdbc.sql("SELECT *, now() AS db_now FROM schedules WHERE id = :id FOR UPDATE")
                .param("id", id)
                .query((rs, n) -> new Due(mapper.mapRow(rs, n), instant(rs, "db_now")))
                .optional();
    }

    public Schedule enable(UUID id, Instant nextFireAt) {
        return jdbc.sql("UPDATE schedules SET enabled = true, next_fire_at = :next WHERE id = :id RETURNING *")
                .param("id", id)
                .param("next", timestamptz(nextFireAt))
                .query(mapper)
                .single();
    }

    /**
     * Locks up to {@code limit} due schedules for the rest of the transaction. SKIP LOCKED
     * makes concurrent firers take disjoint sets instead of waiting on each other, and a
     * firer that arrives after another committed sees the advanced next_fire_at and finds
     * the schedule no longer due.
     */
    public List<Due> lockDue(int limit) {
        return jdbc.sql("""
                        SELECT *, now() AS db_now FROM schedules
                        WHERE enabled AND next_fire_at <= now()
                        ORDER BY next_fire_at
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query((rs, n) -> new Due(mapper.mapRow(rs, n), instant(rs, "db_now")))
                .list();
    }

    /** Records a firing and moves the schedule to its next fire time. */
    public void advance(UUID id, Instant nextFireAt) {
        jdbc.sql("UPDATE schedules SET last_fired_at = now(), next_fire_at = :next WHERE id = :id")
                .param("id", id)
                .param("next", timestamptz(nextFireAt))
                .update();
    }

    /** For a schedule that can never fire again. */
    public void disable(UUID id) {
        jdbc.sql("UPDATE schedules SET enabled = false WHERE id = :id").param("id", id).update();
    }

    private static OffsetDateTime timestamptz(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
