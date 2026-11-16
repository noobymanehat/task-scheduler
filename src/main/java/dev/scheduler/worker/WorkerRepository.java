package dev.scheduler.worker;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerRepository {

    private final JdbcClient jdbc;

    public WorkerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void register(UUID id, String hostname) {
        jdbc.sql("INSERT INTO workers (id, hostname) VALUES (:id, :hostname)")
                .param("id", id)
                .param("hostname", hostname)
                .update();
    }

    /**
     * Refreshes the heartbeat. Returns false if this worker is no longer ALIVE, which means
     * a reaper declared it dead and took its jobs: the caller must stop acting under this id.
     */
    public boolean heartbeat(UUID id) {
        return jdbc.sql("UPDATE workers SET last_heartbeat_at = now() WHERE id = :id AND status = 'ALIVE'")
                        .param("id", id)
                        .update()
                == 1;
    }

    /**
     * ALIVE -> DEAD for every worker whose heartbeat is older than {@code timeout}, judged by
     * the database clock. SKIP LOCKED lets concurrent reapers pass over rows another reaper
     * is already handling.
     */
    public int markDeadWhereHeartbeatExpired(Duration timeout) {
        return jdbc.sql("""
                        WITH expired AS (
                            SELECT id FROM workers
                            WHERE status = 'ALIVE'
                              AND last_heartbeat_at < now() - :timeoutMillis * interval '1 millisecond'
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE workers w SET status = 'DEAD' FROM expired WHERE w.id = expired.id
                        """)
                .param("timeoutMillis", timeout.toMillis())
                .update();
    }

    /** ALIVE -> STOPPED. */
    public boolean markStopped(UUID id) {
        return jdbc.sql("UPDATE workers SET status = 'STOPPED' WHERE id = :id AND status = 'ALIVE'")
                        .param("id", id)
                        .update()
                == 1;
    }

    /** Most recently started first. {@code status} may be null for all. */
    public List<WorkerInfo> list(String status, int limit) {
        return jdbc.sql("""
                        SELECT w.*,
                               (SELECT count(*) FROM jobs j
                                WHERE j.locked_by = w.id AND j.status = 'RUNNING') AS running_jobs
                        FROM workers w
                        WHERE CAST(:status AS text) IS NULL OR w.status = :status
                        ORDER BY w.started_at DESC, w.id
                        LIMIT :limit
                        """)
                .param("status", status)
                .param("limit", limit)
                .query((rs, n) -> new WorkerInfo(
                        rs.getObject("id", UUID.class),
                        rs.getString("hostname"),
                        rs.getString("status"),
                        rs.getObject("started_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("last_heartbeat_at", OffsetDateTime.class).toInstant(),
                        rs.getLong("running_jobs")))
                .list();
    }
}
