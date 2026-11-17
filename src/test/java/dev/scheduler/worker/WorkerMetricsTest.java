package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobStatus;
import dev.scheduler.metrics.QueueStats;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Runs a workload whose outcome is known exactly and checks the metrics say the same. */
class WorkerMetricsTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Test
    void countersAndTimersMatchAKnownWorkload() {
        for (int i = 0; i < 5; i++) {
            submit("ok");
        }
        Job doomed = submitWithMaxRetries("doomed", 2); // three failed attempts, then dead
        Job flaky = submitWithMaxRetries("flaky", 3); // one failure, then success

        startWorker(
                4,
                TestHandlers.of("ok", ctx -> {}),
                TestHandlers.of("doomed", ctx -> {
                    throw new IllegalStateException("always");
                }),
                TestHandlers.of("flaky", ctx -> {
                    if (ctx.attempt() == 1) {
                        throw new IllegalStateException("once");
                    }
                }));
        await().atMost(TIMEOUT)
                .until(() -> countJobs("SUCCEEDED") == 6 && reload(doomed).status() == JobStatus.DEAD);
        assertThat(reload(flaky).status()).isEqualTo(JobStatus.SUCCEEDED);

        assertThat(counter("jobs.claimed", "ok")).isEqualTo(5);
        assertThat(counter("jobs.succeeded", "ok")).isEqualTo(5);
        assertThat(counter("jobs.failed", "ok")).isZero();

        assertThat(counter("jobs.claimed", "doomed")).isEqualTo(3);
        assertThat(counter("jobs.failed", "doomed")).isEqualTo(3);
        assertThat(counter("jobs.retried", "doomed")).isEqualTo(2);
        assertThat(counter("jobs.dead", "doomed")).isEqualTo(1);
        assertThat(counter("jobs.succeeded", "doomed")).isZero();

        assertThat(counter("jobs.claimed", "flaky")).isEqualTo(2);
        assertThat(counter("jobs.failed", "flaky")).isEqualTo(1);
        assertThat(counter("jobs.retried", "flaky")).isEqualTo(1);
        assertThat(counter("jobs.dead", "flaky")).isZero();
        assertThat(counter("jobs.succeeded", "flaky")).isEqualTo(1);

        // One execution and one queue-wait sample per claim.
        assertThat(meters.get("job.execution.duration").tag("type", "ok").tag("outcome", "succeeded").timer().count())
                .isEqualTo(5);
        assertThat(meters.get("job.execution.duration").tag("type", "doomed").tag("outcome", "failed").timer().count())
                .isEqualTo(3);
        assertThat(meters.get("job.queue.wait").tag("type", "ok").timer().count()).isEqualTo(5);
        assertThat(meters.get("job.queue.wait").tag("type", "doomed").timer().count()).isEqualTo(3);
        assertThat(meters.get("claim.query.duration").timer().count()).isPositive();
    }

    @Test
    void executionTimerMeasuresHandlerTimeAndQueueWaitMeasuresLateness() {
        // Due about 3 seconds ago. run_at comes from this JVM's clock and the claim time from
        // the database's, so allow for a little skew between the two.
        submit("slow", 0, Instant.now().minusSeconds(3));

        startWorker(1, TestHandlers.of("slow", ctx -> Thread.sleep(150)));
        await().atMost(TIMEOUT).until(() -> countJobs("SUCCEEDED") == 1);

        var execution = meters.get("job.execution.duration").tag("type", "slow").timer();
        assertThat(execution.totalTime(TimeUnit.MILLISECONDS)).isBetween(140.0, 5000.0);
        var wait = meters.get("job.queue.wait").tag("type", "slow").timer();
        assertThat(wait.totalTime(TimeUnit.MILLISECONDS)).isBetween(2500.0, 60000.0);
    }

    @Test
    void timeoutReleaseAndReclaimAreCounted() throws Exception {
        // Timeout: one attempt allowed, so the timeout also dead-letters it.
        Job hung = submitWithTimeout("hangs", 0, 1);
        Worker worker = startWorker(1, TestHandlers.of("hangs", ctx -> new CountDownLatch(1).await()));
        await().atMost(TIMEOUT).until(() -> reload(hung).status() == JobStatus.DEAD);
        assertThat(counter("jobs.timed_out", "hangs")).isEqualTo(1);
        assertThat(counter("jobs.dead", "hangs")).isEqualTo(1);
        assertThat(counter("jobs.failed", "hangs")).as("a timeout is not also counted as a handler failure").isZero();
        await().atMost(TIMEOUT).until(() -> worker.stuckHandlers() == 0);
        assertThat(meters.get("jobs.handlers.stuck").gauge().value()).isZero();
        worker.stop();

        // Release at shutdown.
        var started = new CountDownLatch(1);
        submit("blocks");
        Worker draining = startWorker(props(1, Duration.ofMillis(100)), TestHandlers.of("blocks", ctx -> {
            started.countDown();
            new CountDownLatch(1).await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        draining.stop();
        assertThat(counter("jobs.released", "blocks")).isEqualTo(1);

        // Reclaim from a dead worker: the released job, claimed by a worker that then dies.
        UUID victim = registeredWorkerId();
        assertThat(jobs.claim(victim, 1)).hasSize(1);
        expireHeartbeat(victim);
        reaper(Duration.ofSeconds(30)).reapOnce();
        assertThat(meters.counter("jobs.reclaimed", "result", "requeued").count()).isEqualTo(1.0);
        assertThat(meters.counter("jobs.reclaimed", "result", "dead").count()).isZero();
    }

    @Test
    void queueGaugesReflectTheDatabase() {
        var stats = new QueueStats(jdbc, meters, Duration.ZERO); // no caching: every read queries
        UUID worker = registeredWorkerId();
        registeredWorkerId();
        UUID stopped = registeredWorkerId();
        workers.markStopped(stopped);

        assertThat(depth("ready")).isZero();
        assertThat(meters.get("jobs.oldest.pending.age").gauge().value()).isZero();

        submit("t", 0, Instant.now().minusSeconds(120)); // ready, 2 minutes old
        submit("t");
        submit("t");
        submit("t", 0, Instant.now().plusSeconds(3600)); // scheduled
        Job toKill = submit("t", 100, null);
        Job claimed = jobs.claim(worker, 1).getFirst(); // highest priority: toKill
        assertThat(claimed.id()).isEqualTo(toKill.id());
        jobs.markFailed(claimed.id(), worker, 1, "x", false, Duration.ZERO);
        jobs.claim(worker, 1); // the 2-minute-old one is now running

        assertThat(depth("ready")).isEqualTo(2);
        assertThat(depth("scheduled")).isEqualTo(1);
        assertThat(depth("running")).isEqualTo(1);
        assertThat(depth("dead")).isEqualTo(1);
        assertThat(meters.get("workers.alive").gauge().value()).isEqualTo(2);
        assertThat(meters.get("jobs.oldest.pending.age").gauge().value()).isBetween(0.0, 60.0);
        assertThat(stats).isNotNull();
    }

    @Test
    void queueGaugesAreCachedForTheRefreshInterval() {
        new QueueStats(jdbc, meters, Duration.ofHours(1));
        assertThat(depth("ready")).isZero(); // loads and caches

        submit("t");

        assertThat(depth("ready")).as("still the cached value").isZero();
    }

    private double counter(String name, String type) {
        var found = meters.find(name).tag("type", type).counter();
        return found == null ? 0 : found.count();
    }

    private double depth(String state) {
        return meters.get("jobs.queue.depth").tag("state", state).gauge().value();
    }
}
