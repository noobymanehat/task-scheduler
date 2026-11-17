package dev.scheduler.worker;

import dev.scheduler.IntegrationTest;
import dev.scheduler.job.Job;
import dev.scheduler.job.JobRepository;
import dev.scheduler.job.NewJob;
import dev.scheduler.metrics.SchedulerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;

/** Helpers for tests that drive workers and the claim SQL directly. */
abstract class WorkerTestSupport extends IntegrationTest {

    @Autowired
    protected JobRepository jobs;

    @Autowired
    protected WorkerRepository workers;

    /** A private registry per test, so metric assertions see only this test's activity. */
    protected final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    protected final SchedulerMetrics metrics = new SchedulerMetrics(meters);

    private final List<Worker> started = new ArrayList<>();

    /** No Spring-managed worker runs in this context, so the registry can be emptied too. */
    @BeforeEach
    void emptyWorkers() {
        jdbc.sql("DELETE FROM workers").update();
    }

    @AfterEach
    void stopWorkers() {
        started.forEach(Worker::stop);
        started.clear();
    }

    /** Fast timings for tests: 10 ms polls, 20 ms heartbeats, 1-5 ms retry backoff. */
    protected static WorkerProperties props(int concurrency, Duration shutdownGrace) {
        return new WorkerProperties(
                true,
                concurrency,
                Duration.ofMillis(10),
                shutdownGrace,
                Duration.ofMillis(20),
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                new WorkerProperties.Retry(Duration.ofMillis(1), Duration.ofMillis(5)));
    }

    /** A running worker with a 10 s shutdown grace; stopped automatically after the test. */
    protected Worker startWorker(int concurrency, JobHandler... handlers) {
        return startWorker(props(concurrency, Duration.ofSeconds(10)), handlers);
    }

    protected Worker startWorker(WorkerProperties properties, JobHandler... handlers) {
        var worker = new Worker(jobs, workers, new HandlerRegistry(List.of(handlers)), properties, metrics);
        worker.start();
        started.add(worker);
        return worker;
    }

    /** A reaper that is not running on a schedule; tests call {@code reapOnce()}. */
    protected Reaper reaper(Duration heartbeatTimeout) {
        return new Reaper(workers, jobs, heartbeatTimeout, Duration.ofHours(1), metrics);
    }

    /** Makes a worker look as if it last heartbeated an hour ago. */
    protected void expireHeartbeat(UUID workerId) {
        jdbc.sql("UPDATE workers SET last_heartbeat_at = now() - interval '1 hour' WHERE id = :id")
                .param("id", workerId)
                .update();
    }

    protected String workerStatus(UUID workerId) {
        return jdbc.sql("SELECT status FROM workers WHERE id = :id")
                .param("id", workerId)
                .query(String.class)
                .single();
    }

    protected Job submitWithTimeout(String type, int maxRetries, int timeoutSeconds) {
        return jobs.insert(new NewJob(type, "{}", 0, null, maxRetries, timeoutSeconds, null)).orElseThrow();
    }

    /** A registered worker id with no running worker behind it, for driving the SQL by hand. */
    protected UUID registeredWorkerId() {
        UUID id = UUID.randomUUID();
        workers.register(id, "test");
        return id;
    }

    protected Job submit(String type) {
        return submit(type, 0, null);
    }

    protected Job submit(String type, int priority, Instant runAt) {
        return jobs.insert(new NewJob(type, "{}", priority, runAt, 3, 300, null)).orElseThrow();
    }

    protected Job submitWithMaxRetries(String type, int maxRetries) {
        return jobs.insert(new NewJob(type, "{}", 0, null, maxRetries, 300, null)).orElseThrow();
    }

    protected Job reload(Job job) {
        return jobs.findById(job.id()).orElseThrow();
    }

    protected long countJobs(String status) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE status = :s").param("s", status).query(Long.class).single();
    }
}
