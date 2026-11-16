package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    // ---- the failure transition, driven directly ----

    @Test
    void retryableFailureWithBudgetLeftReschedulesTheJobAfterTheDelay() {
        UUID worker = registeredWorkerId();
        Job job = claim(worker, submitWithMaxRetries("t", 3));

        Optional<JobStatus> result =
                jobs.markFailed(job.id(), worker, job.attempts(), "first failure", true, Duration.ofSeconds(30));

        assertThat(result).contains(JobStatus.PENDING);
        Job retrying = reload(job);
        assertThat(retrying.status()).isEqualTo(JobStatus.PENDING);
        assertThat(retrying.lastError()).isEqualTo("first failure");
        assertThat(retrying.finishedAt()).isNull();
        assertThat(retrying.lockedBy()).isNull();
        assertThat(retrying.attempts()).isEqualTo(1);
        assertThat(secondsUntilRunAt(job)).isBetween(29.0, 30.0);
        assertThat(jobs.findAttempts(job.id())).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("FAILED");
            assertThat(attempt.error()).isEqualTo("first failure");
            assertThat(attempt.finishedAt()).isNotNull();
        });
        // Backoff is just a future run_at: the job is not claimable until then.
        assertThat(jobs.claim(worker, 10)).isEmpty();
    }

    @Test
    void failureOnTheLastAllowedAttemptSendsTheJobToTheDeadLetterQueue() {
        UUID worker = registeredWorkerId();
        Job submitted = submitWithMaxRetries("t", 2);

        // max_retries = 2 allows three attempts in total.
        assertThat(failOnce(worker, submitted)).contains(JobStatus.PENDING);
        assertThat(failOnce(worker, submitted)).contains(JobStatus.PENDING);
        assertThat(failOnce(worker, submitted)).contains(JobStatus.DEAD);

        Job dead = reload(submitted);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.lockedBy()).isNull();
        assertThat(jobs.findAttempts(dead.id()))
                .extracting(JobAttempt::attempt, JobAttempt::outcome)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1, "FAILED"),
                        org.assertj.core.groups.Tuple.tuple(2, "FAILED"),
                        org.assertj.core.groups.Tuple.tuple(3, "FAILED"));
        assertThat(jobs.claim(worker, 10)).isEmpty();
    }

    @Test
    void nonRetryableFailureGoesDeadEvenWithBudgetLeft() {
        UUID worker = registeredWorkerId();
        Job job = claim(worker, submitWithMaxRetries("t", 5));

        Optional<JobStatus> result =
                jobs.markFailed(job.id(), worker, job.attempts(), "bad payload", false, Duration.ofSeconds(30));

        assertThat(result).contains(JobStatus.DEAD);
        assertThat(reload(job).status()).isEqualTo(JobStatus.DEAD);
    }

    @Test
    void excusedAttemptsDoNotCountAgainstTheBudget() {
        UUID worker = registeredWorkerId();
        Job submitted = submitWithMaxRetries("t", 0);
        // Two earlier attempts were excused (e.g. released at shutdown): this is counted attempt 1.
        jdbc.sql("UPDATE jobs SET attempts = 2, attempts_excused = 2 WHERE id = :id")
                .param("id", submitted.id())
                .update();
        // And for a job whose budget is spent, the excused ones still do not rescue it.
        Job other = submitWithMaxRetries("t", 1);
        jdbc.sql("UPDATE jobs SET attempts = 3, attempts_excused = 2, priority = -1 WHERE id = :id")
                .param("id", other.id())
                .update();

        List<Job> claimed = jobs.claim(worker, 2);
        Job first = claimed.get(0);
        Job second = claimed.get(1);

        // first: attempt 3, counted 1, max_retries 0 -> out of budget after this failure
        assertThat(jobs.markFailed(first.id(), worker, 3, "x", true, Duration.ZERO)).contains(JobStatus.DEAD);
        // second: attempt 4, counted 2, max_retries 1 -> out of budget
        assertThat(jobs.markFailed(second.id(), worker, 4, "x", true, Duration.ZERO)).contains(JobStatus.DEAD);

        // With one more retry allowed, the same counts would have rescheduled instead.
        Job roomy = submitWithMaxRetries("t", 1);
        jdbc.sql("UPDATE jobs SET attempts = 2, attempts_excused = 2 WHERE id = :id")
                .param("id", roomy.id())
                .update();
        Job roomyClaimed = jobs.claim(worker, 1).getFirst();
        assertThat(jobs.markFailed(roomyClaimed.id(), worker, 3, "x", true, Duration.ZERO))
                .contains(JobStatus.PENDING);
    }

    // ---- real workers ----

    @Test
    void alwaysFailingJobRunsMaxRetriesPlusOneTimesThenDies() {
        var runs = new AtomicInteger();
        Job job = submitWithMaxRetries("always-fails", 3);

        startWorker(2, TestHandlers.of("always-fails", ctx -> {
            assertThat(ctx.attempt()).isEqualTo(runs.incrementAndGet());
            throw new IllegalStateException("attempt " + ctx.attempt() + " failed");
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        assertThat(runs.get()).isEqualTo(4);
        Job dead = reload(job);
        assertThat(dead.attempts()).isEqualTo(4);
        assertThat(dead.lastError()).contains("attempt 4 failed");
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::outcome).containsOnly("FAILED").hasSize(4);
        // It stays dead: nothing picks it up again.
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).until(() -> runs.get() == 4);
    }

    @Test
    void jobThatFailsTwiceThenSucceedsEndsSucceeded() {
        var runs = new AtomicInteger();
        Job job = submitWithMaxRetries("flaky", 3);

        startWorker(2, TestHandlers.of("flaky", ctx -> {
            if (runs.incrementAndGet() < 3) {
                throw new IllegalStateException("not yet");
            }
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        assertThat(runs.get()).isEqualTo(3);
        assertThat(jobs.findAttempts(job.id()))
                .extracting(JobAttempt::outcome)
                .containsExactly("FAILED", "FAILED", "SUCCEEDED");
    }

    @Test
    void handlerThrowingNonRetryableExceptionIsNotRetried() {
        var runs = new AtomicInteger();
        Job job = submitWithMaxRetries("invalid", 5);

        startWorker(1, TestHandlers.of("invalid", ctx -> {
            runs.incrementAndGet();
            throw new NonRetryableException("payload can never be processed");
        }));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        assertThat(runs.get()).isEqualTo(1);
        assertThat(reload(job).attempts()).isEqualTo(1);
        assertThat(reload(job).lastError()).contains("payload can never be processed");
    }

    // ---- helpers ----

    private Job claim(UUID worker, Job expected) {
        List<Job> claimed = jobs.claim(worker, 1);
        assertThat(claimed).extracting(Job::id).containsExactly(expected.id());
        return claimed.getFirst();
    }

    /** Claims the job (it is the only due one) and fails it with zero backoff. */
    private Optional<JobStatus> failOnce(UUID worker, Job job) {
        Job claimed = claim(worker, job);
        return jobs.markFailed(claimed.id(), worker, claimed.attempts(), "failed", true, Duration.ZERO);
    }

    private double secondsUntilRunAt(Job job) {
        return jdbc.sql("SELECT extract(epoch FROM run_at - now()) FROM jobs WHERE id = :id")
                .param("id", job.id())
                .query(Double.class)
                .single();
    }
}
