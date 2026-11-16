package dev.scheduler.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled       whether this process runs a worker
 * @param concurrency   maximum number of jobs executing at once in this worker
 * @param pollInterval  pause between claim queries when the queue did not fill a batch
 * @param shutdownGrace how long shutdown waits for in-flight jobs
 */
@ConfigurationProperties("scheduler.worker")
public record WorkerProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("10") int concurrency,
        @DefaultValue("1s") Duration pollInterval,
        @DefaultValue("30s") Duration shutdownGrace) {

    public WorkerProperties {
        if (concurrency < 1) {
            throw new IllegalArgumentException("scheduler.worker.concurrency must be at least 1");
        }
    }
}
