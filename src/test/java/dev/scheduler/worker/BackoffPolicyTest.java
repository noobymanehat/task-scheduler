package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Random;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    private static final WorkerProperties.Retry RETRY =
            new WorkerProperties.Retry(Duration.ofSeconds(5), Duration.ofHours(1));

    @Test
    void ceilingDoublesPerAttemptStartingAtTheBaseDelay() {
        var policy = new BackoffPolicy(RETRY, new Random(1));

        assertThat(policy.ceiling(1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(policy.ceiling(2)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.ceiling(3)).isEqualTo(Duration.ofSeconds(20));
        assertThat(policy.ceiling(4)).isEqualTo(Duration.ofSeconds(40));
        assertThat(policy.ceiling(10)).isEqualTo(Duration.ofSeconds(2560));
    }

    @Test
    void ceilingIsCappedAtTheMaxDelayAndNeverOverflows() {
        var policy = new BackoffPolicy(RETRY, new Random(1));

        assertThat(policy.ceiling(11)).isEqualTo(Duration.ofHours(1)); // 5120 s uncapped
        assertThat(policy.ceiling(64)).isEqualTo(Duration.ofHours(1));
        assertThat(policy.ceiling(5000)).isEqualTo(Duration.ofHours(1));
        assertThat(policy.ceiling(Integer.MAX_VALUE)).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void delayIsAlwaysBetweenZeroAndTheCeilingAndUsesTheWholeRange() {
        var policy = new BackoffPolicy(RETRY, new Random(42));

        for (int attempt = 1; attempt <= 15; attempt++) {
            Duration ceiling = policy.ceiling(attempt);
            Duration min = ceiling;
            Duration max = Duration.ZERO;
            for (int i = 0; i < 2000; i++) {
                Duration delay = policy.delay(attempt);
                assertThat(delay).isBetween(Duration.ZERO, ceiling);
                min = delay.compareTo(min) < 0 ? delay : min;
                max = delay.compareTo(max) > 0 ? delay : max;
            }
            // Full jitter: samples reach into both the bottom and top tenth of the interval.
            assertThat(min).isLessThan(ceiling.dividedBy(10));
            assertThat(max).isGreaterThan(ceiling.multipliedBy(9).dividedBy(10));
        }
    }

    @Test
    void delayCanReachBothEndsOfTheInterval() {
        var alwaysLowest = new BackoffPolicy(RETRY, fixed(0));
        var alwaysHighest = new BackoffPolicy(RETRY, fixed(Long.MAX_VALUE));

        assertThat(alwaysLowest.delay(3)).isEqualTo(Duration.ZERO);
        assertThat(alwaysHighest.delay(3)).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void zeroBaseDelayMeansImmediateRetry() {
        var policy = new BackoffPolicy(new WorkerProperties.Retry(Duration.ZERO, Duration.ZERO), new Random(1));

        assertThat(policy.delay(1)).isEqualTo(Duration.ZERO);
        assertThat(policy.delay(9)).isEqualTo(Duration.ZERO);
    }

    /** A generator whose bounded draws always land at the lowest or highest allowed value. */
    private static RandomGenerator fixed(long value) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                return value;
            }

            @Override
            public long nextLong(long bound) {
                return value == 0 ? 0 : bound - 1;
            }
        };
    }
}
