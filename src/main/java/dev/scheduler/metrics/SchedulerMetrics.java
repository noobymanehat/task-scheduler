package dev.scheduler.metrics;

import dev.scheduler.job.Job;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Every event-driven metric the scheduler emits, in one place so names and tags stay
 * consistent. Counters and timers are tagged with the job {@code type}; types come from
 * registered handlers, so the tag's cardinality is bounded by the codebase, not by users.
 */
@Component
public class SchedulerMetrics {

    private final MeterRegistry registry;

    public SchedulerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void submitted(String type) {
        count("jobs.submitted", type);
    }

    /** Counts the claim and records how long the job waited past its run-at time. */
    public void claimed(Job job) {
        count("jobs.claimed", job.type());
        if (job.lockedAt() != null) {
            Duration wait = Duration.between(job.runAt(), job.lockedAt());
            Timer.builder("job.queue.wait")
                    .description("Time between a job becoming due and a worker claiming it")
                    .tag("type", job.type())
                    .register(registry)
                    .record(wait.isNegative() ? Duration.ZERO : wait);
        }
    }

    public void succeeded(String type, Duration execution) {
        count("jobs.succeeded", type);
        recordExecution(type, "succeeded", execution);
    }

    /** A handler failure. {@code dead} says whether it exhausted the job's budget. */
    public void failed(String type, Duration execution, boolean dead) {
        count("jobs.failed", type);
        count(dead ? "jobs.dead" : "jobs.retried", type);
        recordExecution(type, "failed", execution);
    }

    public void timedOut(String type, boolean dead) {
        count("jobs.timed_out", type);
        count(dead ? "jobs.dead" : "jobs.retried", type);
    }

    public void released(String type) {
        count("jobs.released", type);
    }

    /** Jobs taken back from dead workers by the reaper. */
    public void reclaimed(int requeued, int dead) {
        registry.counter("jobs.reclaimed", "result", "requeued").increment(requeued);
        registry.counter("jobs.reclaimed", "result", "dead").increment(dead);
    }

    public void schedulesFired(int jobsCreated) {
        registry.counter("schedules.fired").increment(jobsCreated);
    }

    public <T> T timeClaim(Supplier<T> claim) {
        return registry.timer("claim.query.duration").record(claim);
    }

    /** Handlers that timed out but have not returned; each still holds an executor slot. */
    public void trackStuckHandlers(Supplier<Number> stuck) {
        Gauge.builder("jobs.handlers.stuck", stuck)
                .description("Timed-out handlers that have not yet returned")
                .register(registry);
    }

    private void recordExecution(String type, String outcome, Duration execution) {
        Timer.builder("job.execution.duration")
                .description("Wall-clock time spent in the job handler")
                .tag("type", type)
                .tag("outcome", outcome)
                .register(registry)
                .record(execution);
    }

    private void count(String name, String type) {
        registry.counter(name, "type", type).increment();
    }
}
