package dev.scheduler.worker;

import dev.scheduler.IntegrationTest;
import dev.scheduler.job.Job;
import dev.scheduler.job.JobRepository;
import dev.scheduler.job.NewJob;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;

/** Helpers for tests that drive workers and the claim SQL directly. */
abstract class WorkerTestSupport extends IntegrationTest {

    @Autowired
    protected JobRepository jobs;

    @Autowired
    protected WorkerRepository workers;

    private final List<Worker> started = new ArrayList<>();

    @AfterEach
    void stopWorkers() {
        started.forEach(Worker::stop);
        started.clear();
    }

    /** A running worker polling every 10 ms with 1-5 ms retry backoff; stopped automatically after the test. */
    protected Worker startWorker(int concurrency, JobHandler... handlers) {
        var worker = new Worker(
                jobs,
                workers,
                new HandlerRegistry(List.of(handlers)),
                new WorkerProperties(
                        true,
                        concurrency,
                        Duration.ofMillis(10),
                        Duration.ofSeconds(10),
                        new WorkerProperties.Retry(Duration.ofMillis(1), Duration.ofMillis(5))));
        worker.start();
        started.add(worker);
        return worker;
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
