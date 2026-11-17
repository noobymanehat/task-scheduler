package dev.scheduler.worker;

import dev.scheduler.job.JobRepository;
import dev.scheduler.job.JobStatus;
import dev.scheduler.metrics.SchedulerMetrics;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Crash recovery. Declares workers dead when their heartbeat expires and returns their
 * RUNNING jobs to the queue (or to the dead-letter queue if the retry budget is spent).
 *
 * <p>Every worker process runs one; there is no leader. Both steps are idempotent and use
 * row locks, so any number of reapers can run at once and each job is reclaimed once.
 */
public class Reaper implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final WorkerRepository workers;
    private final JobRepository jobs;
    private final Duration heartbeatTimeout;
    private final Duration interval;
    private final SchedulerMetrics metrics;

    private volatile boolean running;
    private Thread thread;

    public Reaper(
            WorkerRepository workers,
            JobRepository jobs,
            Duration heartbeatTimeout,
            Duration interval,
            SchedulerMetrics metrics) {
        this.metrics = metrics;
        this.workers = workers;
        this.jobs = jobs;
        this.heartbeatTimeout = heartbeatTimeout;
        this.interval = interval;
    }

    public record ReapResult(int workersMarkedDead, int jobsRequeued, int jobsDead) {}

    /**
     * One pass. The two steps are separate statements on purpose: the second sweeps the jobs
     * of every non-ALIVE worker, not just the ones this pass marked, so a reaper that dies
     * between the steps (or a worker that claimed a job while being declared dead) is
     * cleaned up by the next pass of any reaper.
     */
    public ReapResult reapOnce() {
        int markedDead = workers.markDeadWhereHeartbeatExpired(heartbeatTimeout);
        List<JobStatus> reclaimed = jobs.reclaimFromDeadWorkers();
        int requeued = (int) reclaimed.stream().filter(s -> s == JobStatus.PENDING).count();
        var result = new ReapResult(markedDead, requeued, reclaimed.size() - requeued);
        metrics.reclaimed(result.jobsRequeued(), result.jobsDead());
        if (markedDead > 0 || !reclaimed.isEmpty()) {
            log.warn("Reaper: {} worker(s) declared dead, {} job(s) requeued, {} job(s) dead-lettered",
                    result.workersMarkedDead(), result.jobsRequeued(), result.jobsDead());
        }
        return result;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("reaper").daemon(true).start(this::loop);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        thread.interrupt();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        while (running) {
            try {
                // Jitter keeps the fleet's reapers from all firing in the same instant.
                long base = interval.toMillis();
                Thread.sleep(Math.max(1, base + ThreadLocalRandom.current().nextLong(base / 5 + 1)));
                reapOnce();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("Reaper pass failed; will retry", e);
            }
        }
    }
}
