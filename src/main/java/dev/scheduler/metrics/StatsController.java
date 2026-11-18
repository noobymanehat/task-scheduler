package dev.scheduler.metrics;

import dev.scheduler.worker.WorkerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * A snapshot of the queue for the admin UI. It reads the same active-state query as the
 * queue gauges, uncached, so a page polling every couple of seconds sees current values.
 * Finished states (succeeded, cancelled) are left out on purpose: they have no partial index
 * and counting them would scan the whole history (DESIGN.md §9).
 */
@RestController
@RequestMapping("/api/v1/stats")
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class StatsController {

    /**
     * @param heartbeatTimeoutSeconds the configured timeout, so clients can show how close a
     *                                worker's heartbeat age is to getting it declared dead
     */
    public record StatsResponse(
            long ready,
            long scheduled,
            long running,
            long dead,
            double oldestReadySeconds,
            long workersAlive,
            double heartbeatTimeoutSeconds) {}

    private final QueueStats queueStats;
    private final WorkerProperties workerProperties;

    public StatsController(QueueStats queueStats, WorkerProperties workerProperties) {
        this.queueStats = queueStats;
        this.workerProperties = workerProperties;
    }

    @GetMapping
    public StatsResponse stats() {
        QueueStats.Snapshot s = queueStats.read();
        return new StatsResponse(
                s.ready(),
                s.scheduled(),
                s.running(),
                s.dead(),
                s.oldestReadySeconds(),
                s.workersAlive(),
                workerProperties.heartbeatTimeout().toMillis() / 1000.0);
    }
}
