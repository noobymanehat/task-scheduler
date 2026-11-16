package dev.scheduler.worker;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter.
 *
 * <p>After the n-th counted attempt fails, the ceiling is {@code min(maxDelay, baseDelay *
 * 2^(n-1))} and the actual delay is uniform in {@code [0, ceiling]}. Spreading over the whole
 * interval (rather than adding a small jitter to the ceiling) is what stops jobs that failed
 * together, for instance during a downstream outage, from all retrying at the same instant.
 */
public final class BackoffPolicy {

    private final long baseMillis;
    private final long maxMillis;
    private final RandomGenerator random;

    public BackoffPolicy(WorkerProperties.Retry retry, RandomGenerator random) {
        this.baseMillis = retry.baseDelay().toMillis();
        this.maxMillis = retry.maxDelay().toMillis();
        this.random = random;
    }

    /** Upper bound of the delay after {@code countedAttempt} (1-based) failed. */
    public Duration ceiling(int countedAttempt) {
        // Floating point saturates instead of overflowing for large attempt numbers.
        double exponential = baseMillis * Math.pow(2, Math.max(0, countedAttempt - 1));
        return Duration.ofMillis((long) Math.min(maxMillis, exponential));
    }

    /** A random delay in {@code [0, ceiling(countedAttempt)]}. */
    public Duration delay(int countedAttempt) {
        return Duration.ofMillis(random.nextLong(ceiling(countedAttempt).toMillis() + 1));
    }
}
