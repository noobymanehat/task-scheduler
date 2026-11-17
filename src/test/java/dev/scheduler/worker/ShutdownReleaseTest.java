package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ShutdownReleaseTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /**
     * The regression test for attempt-number reuse at shutdown. With max_retries = 1 the job
     * may fail once. Attempt 1 is released at shutdown, attempt 2 fails, attempt 3 succeeds:
     * that only works if the release did not use up budget, and the history only has three
     * distinct rows if the released attempt's number was not handed out again.
     */
    @Test
    void jobStillRunningAfterTheGracePeriodIsReleasedWithoutBurningAnAttempt() throws Exception {
        var started = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        JobHandler handler = TestHandlers.of("work", ctx -> {
            switch (ctx.attempt()) {
                case 1 -> {
                    started.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        throw e;
                    }
                }
                case 2 -> throw new IllegalStateException("fails once after the restart");
                default -> {}
            }
        });
        Job job = submitWithMaxRetries("work", 1);
        Worker workerA = startWorker(props(1, Duration.ofMillis(100)), handler);
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        UUID workerAId = workerA.id();

        workerA.stop();

        Job released = reload(job);
        assertThat(released.status()).isEqualTo(JobStatus.PENDING);
        assertThat(released.lockedBy()).isNull();
        assertThat(released.attempts()).isEqualTo(1);
        assertThat(released.attemptsExcused()).isEqualTo(1);
        assertThat(released.lastError()).isNull();
        assertThat(jobs.findAttempts(job.id())).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("RELEASED");
            assertThat(attempt.finishedAt()).isNotNull();
        });
        assertThat(workerStatus(workerAId)).isEqualTo("STOPPED");
        await().atMost(TIMEOUT).untilTrue(interrupted);

        startWorker(1, handler);

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        Job done = reload(job);
        assertThat(done.attempts()).isEqualTo(3);
        assertThat(done.attemptsExcused()).isEqualTo(1);
        List<JobAttempt> history = jobs.findAttempts(job.id());
        assertThat(history).extracting(JobAttempt::attempt).containsExactly(1, 2, 3);
        assertThat(history).extracting(JobAttempt::outcome).containsExactly("RELEASED", "FAILED", "SUCCEEDED");
    }

    /**
     * The grace period here is several heartbeat timeouts long. A worker that stopped
     * heartbeating when shutdown began would be declared dead while draining: its job would
     * be charged as ABANDONED (and, with max_retries = 0, dead-lettered) instead of released.
     */
    @Test
    void drainingWorkerKeepsHeartbeatingSoItIsNotReapedDuringTheGracePeriod() throws Exception {
        var started = new CountDownLatch(1);
        Job job = submitWithMaxRetries("work", 0);
        Worker worker = startWorker(props(1, Duration.ofSeconds(1)), TestHandlers.of("work", ctx -> {
            started.countDown();
            new CountDownLatch(1).await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        UUID workerId = worker.id();
        Reaper reaper = reaper(Duration.ofMillis(250));

        Thread stopping = Thread.ofPlatform().start(worker::stop);
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(20)).until(() -> {
            assertThat(reaper.reapOnce()).isEqualTo(new Reaper.ReapResult(0, 0, 0));
            return !stopping.isAlive();
        });

        assertThat(workerStatus(workerId)).isEqualTo("STOPPED");
        assertThat(reload(job).status()).isEqualTo(JobStatus.PENDING);
        assertThat(reload(job).attemptsExcused()).isEqualTo(1);
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::outcome).containsExactly("RELEASED");
    }

    @Test
    void releaseIsFencedLikeAnAck() {
        UUID owner = registeredWorkerId();
        UUID other = registeredWorkerId();
        Job job = submit("t");
        Job claimed = jobs.claim(owner, 1).getFirst();

        assertThat(jobs.release(job.id(), other, claimed.attempts())).isFalse();
        assertThat(jobs.release(job.id(), owner, claimed.attempts() + 1)).isFalse();
        assertThat(reload(job).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(reload(job).attemptsExcused()).isZero();

        assertThat(jobs.release(job.id(), owner, claimed.attempts())).isTrue();
        assertThat(jobs.release(job.id(), owner, claimed.attempts())).isFalse();
        assertThat(reload(job).attemptsExcused()).isEqualTo(1);
    }

    @Test
    void stoppedWorkerIsNotReapedAndHoldsNoJobs() throws Exception {
        var started = new CountDownLatch(1);
        Job job = submit("work");
        Worker worker = startWorker(props(1, Duration.ofMillis(100)), TestHandlers.of("work", ctx -> {
            started.countDown();
            new CountDownLatch(1).await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();

        worker.stop();
        expireHeartbeat(worker.id());

        assertThat(reaper(Duration.ofSeconds(30)).reapOnce()).isEqualTo(new Reaper.ReapResult(0, 0, 0));
        assertThat(workerStatus(worker.id())).isEqualTo("STOPPED");
        assertThat(reload(job).status()).isEqualTo(JobStatus.PENDING);
    }
}
