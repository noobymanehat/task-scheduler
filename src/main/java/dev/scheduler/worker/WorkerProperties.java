package dev.scheduler.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled           whether this process runs a worker (and a reaper)
 * @param concurrency       maximum number of jobs executing at once in this worker
 * @param pollInterval      pause between claim queries when the queue did not fill a batch
 * @param shutdownGrace     how long shutdown waits for in-flight jobs before releasing them
 * @param heartbeatInterval how often the worker refreshes its heartbeat
 * @param heartbeatTimeout  heartbeat age beyond which a worker is declared dead
 * @param reaperInterval    how often the reaper looks for dead workers
 * @param retry             backoff between failed attempts
 */
@ConfigurationProperties("scheduler.worker")
public record WorkerProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("10") int concurrency,
        @DefaultValue("1s") Duration pollInterval,
        @DefaultValue("30s") Duration shutdownGrace,
        @DefaultValue("5s") Duration heartbeatInterval,
        @DefaultValue("30s") Duration heartbeatTimeout,
        @DefaultValue("10s") Duration reaperInterval,
        @DefaultValue Retry retry) {

    public WorkerProperties {
        if (concurrency < 1) {
            throw new IllegalArgumentException("scheduler.worker.concurrency must be at least 1");
        }
        if (heartbeatTimeout.compareTo(heartbeatInterval.multipliedBy(2)) < 0) {
            // With less headroom, one delayed heartbeat gets a healthy worker reaped.
            throw new IllegalArgumentException(
                    "scheduler.worker.heartbeat-timeout must be at least twice heartbeat-interval");
        }
    }

    /**
     * @param baseDelay backoff ceiling after the first failed attempt; doubles per attempt
     * @param maxDelay  upper bound on the backoff ceiling
     */
    public record Retry(@DefaultValue("5s") Duration baseDelay, @DefaultValue("1h") Duration maxDelay) {

        public Retry {
            if (baseDelay.isNegative() || maxDelay.compareTo(baseDelay) < 0) {
                throw new IllegalArgumentException(
                        "scheduler.worker.retry requires 0 <= base-delay <= max-delay");
            }
        }
    }
}
