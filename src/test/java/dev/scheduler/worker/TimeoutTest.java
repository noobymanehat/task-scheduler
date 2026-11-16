package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Per-job execution timeout. The shortest timeout is one second, so these tests take a few. */
class TimeoutTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Test
    void hungHandlerIsInterruptedRecordedAsTimedOutRetriedAndFinallyDeadLettered() {
        var runs = new AtomicInteger();
        var interrupts = new AtomicInteger();
        Job job = submitWithTimeout("hangs", 1, 1);

        Worker worker = startWorker(1, TestHandlers.of("hangs", ctx -> {
            runs.incrementAndGet();
            try {
                new CountDownLatch(1).await(); // hangs until interrupted
            } catch (InterruptedException e) {
                interrupts.incrementAndGet();
                throw e;
            }
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        Job dead = reload(job);
        assertThat(dead.attempts()).isEqualTo(2);
        assertThat(dead.lastError()).isEqualTo("Timed out after 1s");
        assertThat(jobs.findAttempts(job.id()))
                .extracting(JobAttempt::outcome)
                .containsExactly("TIMED_OUT", "TIMED_OUT");
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::error).containsOnly("Timed out after 1s");
        await().atMost(TIMEOUT).until(() -> interrupts.get() == 2 && worker.stuckHandlers() == 0);
        assertThat(runs.get()).isEqualTo(2);
    }

    @Test
    void jobThatTimesOutOnceCanSucceedOnRetry() {
        Job job = submitWithTimeout("slow-once", 3, 1);

        startWorker(1, TestHandlers.of("slow-once", ctx -> {
            if (ctx.attempt() == 1) {
                new CountDownLatch(1).await();
            }
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        assertThat(jobs.findAttempts(job.id()))
                .extracting(JobAttempt::outcome)
                .containsExactly("TIMED_OUT", "SUCCEEDED");
    }

    @Test
    void jobFinishingWithinItsTimeoutIsUnaffected() {
        Job job = submitWithTimeout("quick", 0, 1);

        Worker worker = startWorker(1, TestHandlers.of("quick", ctx -> {}));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        // Well past the deadline, the cancelled watchdog has changed nothing.
        await().during(Duration.ofMillis(1300)).atMost(TIMEOUT).until(() -> {
            assertThat(reload(job).status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(worker.stuckHandlers()).isZero();
            return true;
        });
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::outcome).containsExactly("SUCCEEDED");
    }

    /**
     * The documented limit of cooperative timeouts: a handler that ignores interruption cannot
     * be killed. Its job is still failed and requeued on time, but the handler keeps its slot
     * (visible as a stuck handler) until it returns, and what it returns is discarded.
     */
    @Test
    void handlerIgnoringInterruptionIsFailedOnTimeButHoldsItsSlotUntilItReturns() {
        var letGo = new AtomicBoolean();
        var runs = new AtomicInteger();
        Job job = submitWithTimeout("stubborn", 3, 1);

        Worker worker = startWorker(1, TestHandlers.of("stubborn", ctx -> {
            runs.incrementAndGet();
            if (ctx.attempt() == 1) {
                while (!letGo.get()) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException ignored) {
                        // deliberately ignored
                    }
                }
            }
        }));

        // Timed out and back in the queue although the handler is still running.
        await().atMost(TIMEOUT)
                .until(() -> jobs.findAttempts(job.id()).stream().anyMatch(a -> "TIMED_OUT".equals(a.outcome())));
        assertThat(worker.stuckHandlers()).isEqualTo(1);
        // The worker's only slot is still occupied, so the due retry cannot start here.
        await().during(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> {
            assertThat(reload(job).status()).isEqualTo(JobStatus.PENDING);
            assertThat(runs.get()).isEqualTo(1);
            return true;
        });

        letGo.set(true);

        // The late return of attempt 1 is discarded; attempt 2 then runs and succeeds.
        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        assertThat(worker.stuckHandlers()).isZero();
        assertThat(runs.get()).isEqualTo(2);
        assertThat(reload(job).attempts()).isEqualTo(2);
        assertThat(jobs.findAttempts(job.id()))
                .extracting(JobAttempt::outcome)
                .containsExactly("TIMED_OUT", "SUCCEEDED");
    }
}
