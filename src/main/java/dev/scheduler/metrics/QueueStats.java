package dev.scheduler.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Gauges describing the queue as it is right now, read from the database.
 *
 * <p>All gauges share one query whose result is cached for {@code refreshInterval}, so a
 * scrape costs at most one query no matter how many gauges it reads. Only active states
 * are counted (ready, scheduled, running, dead); each is served by a partial index, so the
 * cost does not grow with the history of finished jobs.
 */
public class QueueStats {

    private static final Logger log = LoggerFactory.getLogger(QueueStats.class);

    /** Counts of the active job states and live workers, read in one query. */
    public record Snapshot(
            long ready, long scheduled, long running, long dead, double oldestReadySeconds, long workersAlive) {}

    private final JdbcClient jdbc;
    private final long refreshNanos;
    private Snapshot snapshot = new Snapshot(0, 0, 0, 0, 0, 0);
    private long loadedAt;
    private boolean loaded;

    public QueueStats(JdbcClient jdbc, MeterRegistry registry, Duration refreshInterval) {
        this.jdbc = jdbc;
        this.refreshNanos = refreshInterval.toNanos();
        depthGauge(registry, "ready", "Pending jobs whose run-at time has passed");
        depthGauge(registry, "scheduled", "Pending jobs waiting for a future run-at time (delayed or backing off)");
        depthGauge(registry, "running", "Jobs currently held by a worker");
        depthGauge(registry, "dead", "Jobs in the dead-letter queue");
        Gauge.builder("jobs.oldest.pending.age", this, s -> s.current().oldestReadySeconds())
                .description("How long the oldest ready job has been waiting; rises when workers cannot keep up")
                .baseUnit("seconds")
                .register(registry);
        Gauge.builder("workers.alive", this, s -> s.current().workersAlive())
                .description("Workers with a current heartbeat")
                .register(registry);
    }

    private void depthGauge(MeterRegistry registry, String state, String description) {
        Gauge.builder("jobs.queue.depth", this, s -> switch (state) {
                    case "ready" -> s.current().ready();
                    case "scheduled" -> s.current().scheduled();
                    case "running" -> s.current().running();
                    default -> s.current().dead();
                })
                .description(description)
                .tag("state", state)
                .register(registry);
    }

    synchronized Snapshot current() {
        long now = System.nanoTime();
        if (!loaded || now - loadedAt >= refreshNanos) {
            try {
                snapshot = read();
            } catch (RuntimeException e) {
                // A scrape must not fail because the database is briefly unreachable.
                log.warn("Could not refresh queue statistics; reporting the previous values", e);
            }
            loadedAt = now;
            loaded = true;
        }
        return snapshot;
    }

    /** Reads the current values, bypassing the cache that the gauges use. */
    public Snapshot read() {
        return jdbc.sql("""
                        SELECT
                          (SELECT count(*) FROM jobs WHERE status = 'PENDING' AND run_at <= now()) AS ready,
                          (SELECT count(*) FROM jobs WHERE status = 'PENDING' AND run_at > now()) AS scheduled,
                          (SELECT count(*) FROM jobs WHERE status = 'RUNNING') AS running,
                          (SELECT count(*) FROM jobs WHERE status = 'DEAD') AS dead,
                          (SELECT COALESCE(extract(epoch FROM now() - min(run_at)), 0)
                             FROM jobs WHERE status = 'PENDING' AND run_at <= now()) AS oldest_ready_seconds,
                          (SELECT count(*) FROM workers WHERE status = 'ALIVE') AS workers_alive
                        """)
                .query((rs, n) -> new Snapshot(
                        rs.getLong("ready"),
                        rs.getLong("scheduled"),
                        rs.getLong("running"),
                        rs.getLong("dead"),
                        rs.getDouble("oldest_ready_seconds"),
                        rs.getLong("workers_alive")))
                .single();
    }
}
