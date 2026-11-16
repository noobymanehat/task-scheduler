package dev.scheduler.job;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;

class JobSubmitConcurrencyTest extends IntegrationTest {

    private static final int SUBMITTERS = 20;

    @Autowired
    JobService service;

    /** Twenty threads race to submit the same idempotency key over separate DB connections. */
    @RepeatedTest(5)
    void concurrentSubmitsWithOneKeyCreateExactlyOneJob() throws Exception {
        var request = new SubmitJobRequest("charge", null, null, null, null, null, "race-key");
        var ready = new CountDownLatch(SUBMITTERS);
        var go = new CountDownLatch(1);
        List<Future<JobService.Submission>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(SUBMITTERS)) {
            for (int i = 0; i < SUBMITTERS; i++) {
                Callable<JobService.Submission> task = () -> {
                    ready.countDown();
                    go.await();
                    return service.submit(request);
                };
                futures.add(pool.submit(task));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
        }

        List<JobService.Submission> results = new ArrayList<>();
        for (var future : futures) {
            results.add(future.get());
        }
        assertThat(results).hasSize(SUBMITTERS);
        assertThat(results).filteredOn(JobService.Submission::created).hasSize(1);
        assertThat(results).extracting(s -> s.job().id()).containsOnly(results.getFirst().job().id());
        assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isEqualTo(1);
    }
}
