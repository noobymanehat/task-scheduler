package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import dev.scheduler.job.NewJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real workers with real poller and handler threads against a real Postgres. */
class WorkerTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /** The headline guarantee: many workers, many jobs, every job runs exactly once. */
    @Test
    void eightWorkersExecuteOneThousandJobsExactlyOnceEach() {
        int workerCount = 8;
        int jobCount = 1000;
        Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
        JobHandler handler = TestHandlers.of(
                "count", ctx -> executions.computeIfAbsent(ctx.jobId(), id -> new AtomicInteger()).incrementAndGet());
        List<UUID> submitted = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            submitted.add(submit("count", i % 5, null).id());
        }

        Set<UUID> workerIds = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < workerCount; i++) {
            workerIds.add(startWorker(4, handler).id());
        }

        await().atMost(TIMEOUT).until(() -> countJobs("SUCCEEDED") == jobCount);
        assertThat(executions.keySet()).containsExactlyInAnyOrderElementsOf(submitted);
        assertThat(executions.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));
        assertThat(jdbc.sql("SELECT count(*) FROM job_attempts WHERE attempt = 1 AND outcome = 'SUCCEEDED'")
                        .query(Long.class)
                        .single())
                .isEqualTo(jobCount);
        assertThat(jdbc.sql("SELECT count(*) FROM jobs WHERE attempts <> 1").query(Long.class).single()).isZero();
        // The work really was shared, and only by the workers started here.
        List<UUID> workersThatRanJobs =
                jdbc.sql("SELECT DISTINCT worker_id FROM job_attempts").query(UUID.class).list();
        assertThat(workersThatRanJobs).hasSizeGreaterThan(1);
        assertThat(workerIds).containsAll(workersThatRanJobs);
    }

    @Test
    void handlerReceivesTheJobPayloadAndAttemptNumber() {
        List<JobContext> seen = new ArrayList<>();
        Job job = jobs.insert(new NewJob("echo", "{\"n\": 7}", 0, null, 3, 300, null)).orElseThrow();

        startWorker(1, TestHandlers.of("echo", seen::add));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        assertThat(seen).singleElement().satisfies(ctx -> {
            assertThat(ctx.jobId()).isEqualTo(job.id());
            assertThat(ctx.type()).isEqualTo("echo");
            assertThat(ctx.payload().get("n").asInt()).isEqualTo(7);
            assertThat(ctx.attempt()).isEqualTo(1);
        });
    }

    @Test
    void failingHandlerWithNoRetriesLeavesTheJobDeadWithTheError() {
        Job job = submitWithMaxRetries("boom", 0);

        startWorker(1, TestHandlers.of("boom", ctx -> {
            throw new IllegalStateException("kaboom");
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        Job dead = reload(job);
        assertThat(dead.lastError()).contains("IllegalStateException").contains("kaboom");
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.lockedBy()).isNull();
        assertThat(jobs.findAttempts(job.id())).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("FAILED");
            assertThat(attempt.error()).contains("kaboom");
        });
    }

    @Test
    void jobWithNoRegisteredHandlerGoesDead() {
        Job job = submit("nobody-handles-this");

        startWorker(1);

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        assertThat(reload(job).lastError()).contains("No handler registered").contains("nobody-handles-this");
        // Not retried, although the job had retries left.
        assertThat(reload(job).attempts()).isEqualTo(1);
    }

    @Test
    void oneFailingJobDoesNotStopTheWorker() {
        Job bad = submitWithMaxRetries("flaky", 0);
        Job good = submit("ok", 0, null);

        startWorker(
                1,
                TestHandlers.of("flaky", ctx -> {
                    throw new Error("even an Error must not kill the worker");
                }),
                TestHandlers.of("ok", ctx -> {}));

        await().atMost(TIMEOUT).until(() -> reload(good).status() == JobStatus.SUCCEEDED);
        await().atMost(TIMEOUT).until(() -> reload(bad).status() == JobStatus.DEAD);
    }

    @Test
    void neverRunsMoreJobsAtOnceThanItsConcurrency() {
        int concurrency = 3;
        int jobCount = 30;
        var running = new AtomicInteger();
        var maxRunning = new AtomicInteger();
        for (int i = 0; i < jobCount; i++) {
            submit("slow");
        }

        startWorker(concurrency, TestHandlers.of("slow", ctx -> {
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            // Hold the slot long enough for the poller to try to overfill.
            new CountDownLatch(1).await(20, TimeUnit.MILLISECONDS);
            running.decrementAndGet();
        }));

        // While jobs are in flight, the database never shows more RUNNING than the limit either.
        await().atMost(TIMEOUT).until(() -> {
            assertThat(countJobs("RUNNING")).isLessThanOrEqualTo(concurrency);
            return countJobs("SUCCEEDED") == jobCount;
        });
        assertThat(maxRunning.get()).isEqualTo(concurrency);
    }

    @Test
    void stopWaitsForTheRunningJobAndMarksTheWorkerStopped() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Job job = submit("blocking");
        Job never = submit("blocking", -1, null);
        Worker worker = startWorker(1, TestHandlers.of("blocking", ctx -> {
            started.countDown();
            release.await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();

        Thread stopper = Thread.ofPlatform().start(worker::stop);
        // stop() is waiting on the in-flight job, not abandoning it.
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).until(stopper::isAlive);
        release.countDown();
        stopper.join(TIMEOUT);

        assertThat(stopper.isAlive()).isFalse();
        assertThat(reload(job).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::outcome).containsExactly("SUCCEEDED");
        // Nothing new was claimed once shutdown began.
        assertThat(reload(never).status()).isEqualTo(JobStatus.PENDING);
        assertThat(jdbc.sql("SELECT status FROM workers WHERE id = :id")
                        .param("id", worker.id())
                        .query(String.class)
                        .single())
                .isEqualTo("STOPPED");
    }
}
