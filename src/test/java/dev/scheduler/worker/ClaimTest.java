package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/** The claim and ack SQL, driven directly with no worker threads involved. */
class ClaimTest extends WorkerTestSupport {

    @Test
    void claimMarksJobRunningAndRecordsTheAttempt() {
        UUID worker = registeredWorkerId();
        Job submitted = submit("t");

        List<Job> claimed = jobs.claim(worker, 10);

        assertThat(claimed).hasSize(1);
        Job job = claimed.getFirst();
        assertThat(job.id()).isEqualTo(submitted.id());
        assertThat(job.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(job.lockedBy()).isEqualTo(worker);
        assertThat(job.lockedAt()).isNotNull();
        assertThat(job.attempts()).isEqualTo(1);
        assertThat(jobs.findAttempts(job.id()))
                .singleElement()
                .satisfies(attempt -> {
                    assertThat(attempt.attempt()).isEqualTo(1);
                    assertThat(attempt.workerId()).isEqualTo(worker);
                    assertThat(attempt.startedAt()).isNotNull();
                    assertThat(attempt.finishedAt()).isNull();
                    assertThat(attempt.outcome()).isNull();
                });
    }

    @Test
    void claimsHighestPriorityFirstThenEarliestRunAt() {
        UUID worker = registeredWorkerId();
        Instant now = Instant.now();
        Job lowOld = submit("t", 1, now.minus(3, ChronoUnit.HOURS));
        Job highNewer = submit("t", 9, now.minus(1, ChronoUnit.HOURS));
        Job highOlder = submit("t", 9, now.minus(2, ChronoUnit.HOURS));
        Job mid = submit("t", 5, now.minus(4, ChronoUnit.HOURS));

        List<Job> firstTwo = jobs.claim(worker, 2);
        List<Job> rest = jobs.claim(worker, 10);

        assertThat(firstTwo).extracting(Job::id).containsExactly(highOlder.id(), highNewer.id());
        assertThat(rest).extracting(Job::id).containsExactly(mid.id(), lowOld.id());
    }

    @Test
    void doesNotClaimJobsScheduledForTheFuture() {
        UUID worker = registeredWorkerId();
        Job future = submit("t", 100, Instant.now().plus(1, ChronoUnit.HOURS));
        Job due = submit("t", 0, null);

        List<Job> claimed = jobs.claim(worker, 10);

        assertThat(claimed).extracting(Job::id).containsExactly(due.id());
        assertThat(reload(future).status()).isEqualTo(JobStatus.PENDING);
        assertThat(reload(future).attempts()).isZero();
    }

    @Test
    void claimsOnlyPendingJobsAndNeverMoreThanTheLimit() {
        UUID worker = registeredWorkerId();
        Job cancelled = submit("t");
        jobs.cancel(cancelled.id());
        for (int i = 0; i < 5; i++) {
            submit("t");
        }

        assertThat(jobs.claim(worker, 3)).hasSize(3);
        assertThat(jobs.claim(worker, 3)).hasSize(2);
        assertThat(jobs.claim(worker, 3)).isEmpty();
        assertThat(reload(cancelled).status()).isEqualTo(JobStatus.CANCELLED);
    }

    @Test
    void successAckFinishesTheJobAndClosesTheAttempt() {
        UUID worker = registeredWorkerId();
        Job job = claimOne(worker, submit("t"));

        boolean acked = jobs.markSucceeded(job.id(), worker, job.attempts());

        assertThat(acked).isTrue();
        Job finished = reload(job);
        assertThat(finished.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(finished.finishedAt()).isNotNull();
        assertThat(finished.lockedBy()).isNull();
        assertThat(jobs.findAttempts(job.id())).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("SUCCEEDED");
            assertThat(attempt.finishedAt()).isNotNull();
        });
    }

    /**
     * Worker A stalls, the job is taken back and claimed by worker B, then A wakes up and
     * acks. A's ack must change nothing: B still owns the job.
     */
    @Test
    void staleAckFromPreviousOwnerIsRejected() {
        UUID workerA = registeredWorkerId();
        UUID workerB = registeredWorkerId();
        Job firstClaim = claimOne(workerA, submit("t"));
        returnToPending(firstClaim);
        Job secondClaim = claimOne(workerB, firstClaim);
        assertThat(secondClaim.attempts()).isEqualTo(2);

        boolean staleSuccess = jobs.markSucceeded(firstClaim.id(), workerA, firstClaim.attempts());
        boolean staleFailure = jobs.markDead(firstClaim.id(), workerA, firstClaim.attempts(), "late failure");

        assertThat(staleSuccess).isFalse();
        assertThat(staleFailure).isFalse();
        Job current = reload(firstClaim);
        assertThat(current.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(current.lockedBy()).isEqualTo(workerB);
        assertThat(current.lastError()).isNull();
        assertThat(jobs.findAttempts(current.id())).extracting(JobAttempt::outcome).containsOnlyNulls();

        assertThat(jobs.markSucceeded(secondClaim.id(), workerB, secondClaim.attempts())).isTrue();
        assertThat(reload(firstClaim).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    /** The same worker re-claims the job: only the attempt number tells the two runs apart. */
    @Test
    void staleAckFromSameWorkerButEarlierAttemptIsRejected() {
        UUID worker = registeredWorkerId();
        Job firstClaim = claimOne(worker, submit("t"));
        returnToPending(firstClaim);
        Job secondClaim = claimOne(worker, firstClaim);

        assertThat(jobs.markSucceeded(firstClaim.id(), worker, firstClaim.attempts())).isFalse();
        assertThat(reload(firstClaim).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(jobs.markSucceeded(secondClaim.id(), worker, secondClaim.attempts())).isTrue();
    }

    @Test
    void ackingAnAlreadyFinishedJobIsRejected() {
        UUID worker = registeredWorkerId();
        Job job = claimOne(worker, submit("t"));
        assertThat(jobs.markSucceeded(job.id(), worker, job.attempts())).isTrue();

        assertThat(jobs.markSucceeded(job.id(), worker, job.attempts())).isFalse();
        assertThat(jobs.markDead(job.id(), worker, job.attempts(), "too late")).isFalse();
        assertThat(reload(job).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    /** Eight connections drain 500 jobs in batches; no job may be handed out twice. */
    @Test
    void concurrentClaimersNeverReceiveTheSameJob() throws Exception {
        int claimers = 8;
        int total = 500;
        for (int i = 0; i < total; i++) {
            submit("t");
        }
        List<UUID> workerIds = new ArrayList<>();
        for (int i = 0; i < claimers; i++) {
            workerIds.add(registeredWorkerId());
        }
        var go = new CountDownLatch(1);
        List<Future<List<UUID>>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(claimers)) {
            for (UUID worker : workerIds) {
                Callable<List<UUID>> task = () -> {
                    List<UUID> mine = new ArrayList<>();
                    go.await();
                    List<Job> batch;
                    while (!(batch = jobs.claim(worker, 7)).isEmpty()) {
                        batch.forEach(job -> mine.add(job.id()));
                    }
                    return mine;
                };
                futures.add(pool.submit(task));
            }
            go.countDown();
        }

        List<UUID> all = new ArrayList<>();
        int claimersThatGotWork = 0;
        for (var future : futures) {
            all.addAll(future.get());
            claimersThatGotWork += future.get().isEmpty() ? 0 : 1;
        }
        assertThat(all).hasSize(total).doesNotHaveDuplicates();
        assertThat(claimersThatGotWork).isGreaterThan(1);
        assertThat(countJobs("RUNNING")).isEqualTo(total);
        assertThat(jdbc.sql("SELECT count(*) FROM job_attempts").query(Long.class).single()).isEqualTo(total);
    }

    private Job claimOne(UUID worker, Job expected) {
        List<Job> claimed = jobs.claim(worker, 1);
        assertThat(claimed).extracting(Job::id).containsExactly(expected.id());
        return claimed.getFirst();
    }

    /** Stands in for crash recovery (Phase 4): the job goes back to the queue. */
    private void returnToPending(Job job) {
        jdbc.sql("UPDATE jobs SET status = 'PENDING', locked_by = NULL, locked_at = NULL WHERE id = :id")
                .param("id", job.id())
                .update();
    }
}
