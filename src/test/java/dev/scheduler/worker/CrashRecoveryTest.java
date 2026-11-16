package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CrashRecoveryTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    // ---- heartbeat ----

    @Test
    void runningWorkerKeepsRefreshingItsHeartbeat() {
        Worker worker = startWorker(1);
        Instant first = lastHeartbeat(worker.id());

        await().atMost(TIMEOUT).until(() -> lastHeartbeat(worker.id()).isAfter(first));
        assertThat(workerStatus(worker.id())).isEqualTo("ALIVE");
    }

    @Test
    void liveWorkerRunningAJobLongerThanTheHeartbeatTimeoutIsNotReaped() throws Exception {
        var started = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        Job job = submit("long");
        Worker worker = startWorker(1, TestHandlers.of("long", ctx -> {
            started.countDown();
            finish.await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        Reaper reaper = reaper(Duration.ofSeconds(1));

        // The job runs for longer than the 1 s timeout; heartbeats (every 20 ms) keep it safe.
        await().during(Duration.ofMillis(1500)).atMost(TIMEOUT).until(() -> {
            assertThat(reaper.reapOnce()).isEqualTo(new Reaper.ReapResult(0, 0, 0));
            return true;
        });
        finish.countDown();

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        assertThat(reload(job).attempts()).isEqualTo(1);
        assertThat(workerStatus(worker.id())).isEqualTo("ALIVE");
    }

    // ---- reaper, driven directly ----

    @Test
    void reaperDeclaresExpiredWorkerDeadAndRequeuesItsJobs() {
        UUID deadWorker = registeredWorkerId();
        UUID healthyWorker = registeredWorkerId();
        Job a = submit("t");
        Job b = submit("t");
        assertThat(jobs.claim(deadWorker, 2)).hasSize(2);
        Job untouched = submit("t");
        assertThat(jobs.claim(healthyWorker, 1)).hasSize(1);
        expireHeartbeat(deadWorker);

        Reaper.ReapResult result = reaper(Duration.ofSeconds(30)).reapOnce();

        assertThat(result).isEqualTo(new Reaper.ReapResult(1, 2, 0));
        assertThat(workerStatus(deadWorker)).isEqualTo("DEAD");
        assertThat(workerStatus(healthyWorker)).isEqualTo("ALIVE");
        for (Job job : List.of(a, b)) {
            Job requeued = reload(job);
            assertThat(requeued.status()).isEqualTo(JobStatus.PENDING);
            assertThat(requeued.lockedBy()).isNull();
            // The crashed attempt is charged: attempts stays 1 and nothing is excused.
            assertThat(requeued.attempts()).isEqualTo(1);
            assertThat(requeued.attemptsExcused()).isZero();
            assertThat(requeued.lastError()).contains(deadWorker.toString()).contains("stopped heartbeating");
            assertThat(jobs.findAttempts(job.id())).singleElement().satisfies(attempt -> {
                assertThat(attempt.outcome()).isEqualTo("ABANDONED");
                assertThat(attempt.finishedAt()).isNotNull();
                assertThat(attempt.error()).contains("stopped heartbeating");
            });
        }
        assertThat(reload(untouched).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(reload(untouched).lockedBy()).isEqualTo(healthyWorker);
        // Immediately claimable by a healthy worker, as attempt 2.
        assertThat(jobs.claim(healthyWorker, 10)).extracting(Job::attempts).containsExactly(2, 2);
        // A second pass finds nothing left to do.
        assertThat(reaper(Duration.ofSeconds(30)).reapOnce()).isEqualTo(new Reaper.ReapResult(0, 0, 0));
    }

    @Test
    void concurrentReapersReclaimEachJobExactlyOnce() throws Exception {
        int jobCount = 60;
        int reapers = 8;
        UUID deadWorker = registeredWorkerId();
        for (int i = 0; i < jobCount; i++) {
            submit("t");
        }
        assertThat(jobs.claim(deadWorker, jobCount)).hasSize(jobCount);
        expireHeartbeat(deadWorker);
        var go = new CountDownLatch(1);
        List<Future<Reaper.ReapResult>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(reapers)) {
            for (int i = 0; i < reapers; i++) {
                Reaper reaper = reaper(Duration.ofSeconds(30));
                Callable<Reaper.ReapResult> task = () -> {
                    go.await();
                    return reaper.reapOnce();
                };
                futures.add(pool.submit(task));
            }
            go.countDown();
        }

        int markedDead = 0;
        int requeued = 0;
        for (var future : futures) {
            markedDead += future.get().workersMarkedDead();
            requeued += future.get().jobsRequeued();
        }
        // SKIP LOCKED may leave rows for a later pass; one more pass must find at most the rest.
        requeued += reaper(Duration.ofSeconds(30)).reapOnce().jobsRequeued();
        assertThat(markedDead).isEqualTo(1);
        assertThat(requeued).isEqualTo(jobCount);
        assertThat(countJobs("PENDING")).isEqualTo(jobCount);
        assertThat(jdbc.sql("SELECT count(*) FROM jobs WHERE attempts <> 1 OR attempts_excused <> 0")
                        .query(Long.class)
                        .single())
                .isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM job_attempts WHERE outcome = 'ABANDONED'")
                        .query(Long.class)
                        .single())
                .isEqualTo(jobCount);
    }

    /** A job that takes its worker down every time must not circulate forever. */
    @Test
    void poisonJobThatKeepsKillingItsWorkerEndsInTheDeadLetterQueue() {
        Job poison = submitWithMaxRetries("poison", 2);
        Reaper reaper = reaper(Duration.ofSeconds(30));

        Reaper.ReapResult last = null;
        for (int crash = 1; crash <= 3; crash++) {
            UUID victim = registeredWorkerId();
            assertThat(jobs.claim(victim, 1)).extracting(Job::id).containsExactly(poison.id());
            expireHeartbeat(victim);
            last = reaper.reapOnce();
        }

        assertThat(last).isEqualTo(new Reaper.ReapResult(1, 0, 1));
        Job dead = reload(poison);
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(jobs.findAttempts(poison.id()))
                .extracting(JobAttempt::outcome)
                .containsExactly("ABANDONED", "ABANDONED", "ABANDONED");
        assertThat(jobs.claim(registeredWorkerId(), 1)).isEmpty();
    }

    @Test
    void workerThatIsNotAliveCannotClaim() {
        UUID dead = registeredWorkerId();
        UUID stopped = registeredWorkerId();
        expireHeartbeat(dead);
        reaper(Duration.ofSeconds(30)).reapOnce();
        workers.markStopped(stopped);
        Job job = submit("t");

        assertThat(jobs.claim(dead, 1)).isEmpty();
        assertThat(jobs.claim(stopped, 1)).isEmpty();
        assertThat(reload(job).status()).isEqualTo(JobStatus.PENDING);
        assertThat(reload(job).attempts()).isZero();
    }

    /** Covers a claim that slipped in while the worker was being declared dead. */
    @Test
    void reaperSweepsJobsOfWorkersAlreadyMarkedDead() {
        UUID worker = registeredWorkerId();
        Job job = submit("t");
        assertThat(jobs.claim(worker, 1)).hasSize(1);
        jdbc.sql("UPDATE workers SET status = 'DEAD' WHERE id = :id").param("id", worker).update();

        Reaper.ReapResult result = reaper(Duration.ofSeconds(30)).reapOnce();

        assertThat(result).isEqualTo(new Reaper.ReapResult(0, 1, 0));
        assertThat(reload(job).status()).isEqualTo(JobStatus.PENDING);
    }

    // ---- real workers ----

    /**
     * Worker A dies mid-job. The reaper notices, worker B runs the job to completion, and
     * when A's still-running handler (a zombie) finally returns, its result is discarded.
     */
    @Test
    void jobOfKilledWorkerIsCompletedByAnotherAndTheZombieResultIsIgnored() throws Exception {
        var zombieStarted = new CountDownLatch(1);
        var zombieMayFinish = new CountDownLatch(1);
        var zombieReturned = new CountDownLatch(1);
        JobHandler handler = TestHandlers.of("work", ctx -> {
            if (ctx.attempt() == 1) {
                zombieStarted.countDown();
                zombieMayFinish.await();
                zombieReturned.countDown();
            }
        });
        Job job = submit("work");
        Worker workerA = startWorker(1, handler);
        assertThat(zombieStarted.await(30, TimeUnit.SECONDS)).isTrue();
        UUID workerAId = workerA.id();

        workerA.crash();
        expireHeartbeat(workerAId);
        Worker workerB = startWorker(1, handler);
        assertThat(reaper(Duration.ofSeconds(30)).reapOnce()).isEqualTo(new Reaper.ReapResult(1, 1, 0));

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        zombieMayFinish.countDown();
        assertThat(zombieReturned.await(30, TimeUnit.SECONDS)).isTrue();

        // Give the zombie's ack every chance to land; the record must not change.
        await().during(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> {
            List<JobAttempt> history = jobs.findAttempts(job.id());
            assertThat(history).extracting(JobAttempt::outcome).containsExactly("ABANDONED", "SUCCEEDED");
            assertThat(history.get(0).workerId()).isEqualTo(workerAId);
            assertThat(history.get(1).workerId()).isEqualTo(workerB.id());
            return true;
        });
        Job done = reload(job);
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(workerStatus(workerAId)).isEqualTo("DEAD");
    }

    /**
     * A worker that was only stalled (not dead) finds out it has been declared dead. It must
     * drop its old identity and jobs and continue as a new worker.
     */
    @Test
    void workerDeclaredDeadAbandonsItsJobsAndReRegistersUnderANewId() throws Exception {
        var started = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        Job job = submit("work");
        Worker worker = startWorker(1, TestHandlers.of("work", ctx -> {
            if (ctx.attempt() == 1) {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    throw e;
                }
            }
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        UUID oldId = worker.id();

        // What a reaper does to a worker whose heartbeats stopped arriving.
        jdbc.sql("UPDATE workers SET status = 'DEAD' WHERE id = :id").param("id", oldId).update();
        assertThat(reaper(Duration.ofSeconds(30)).reapOnce()).isEqualTo(new Reaper.ReapResult(0, 1, 0));

        await().atMost(TIMEOUT).until(() -> !worker.id().equals(oldId));
        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        UUID newId = worker.id();
        assertThat(interrupted).isTrue();
        assertThat(workerStatus(oldId)).isEqualTo("DEAD");
        assertThat(workerStatus(newId)).isEqualTo("ALIVE");
        List<JobAttempt> history = jobs.findAttempts(job.id());
        assertThat(history).extracting(JobAttempt::outcome).containsExactly("ABANDONED", "SUCCEEDED");
        assertThat(history.get(0).workerId()).isEqualTo(oldId);
        assertThat(history.get(1).workerId()).isEqualTo(newId);
    }

    private Instant lastHeartbeat(UUID workerId) {
        return workers.list(null, 200).stream()
                .filter(w -> w.id().equals(workerId))
                .findFirst()
                .orElseThrow()
                .lastHeartbeatAt();
    }
}
