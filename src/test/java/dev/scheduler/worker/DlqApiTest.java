package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobAttempt;
import dev.scheduler.job.JobStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureMockMvc
class DlqApiTest extends WorkerTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @Test
    void listsOnlyDeadJobsMostRecentlyDeadFirstAcrossPages() {
        UUID worker = registeredWorkerId();
        submit("still-pending", -100, null);
        List<String> killedInOrder = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            killedInOrder.add(kill(worker, submit("t")).id().toString());
        }

        List<String> seen = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            var request = mvc.get().uri("/api/v1/dlq").param("limit", "2");
            if (cursor != null) {
                request = request.param("cursor", cursor);
            }
            MvcTestResult result = request.exchange();
            assertThat(result).hasStatusOk();
            JsonNode page = body(result);
            page.get("items").forEach(item -> {
                assertThat(item.get("status").asString()).isEqualTo("DEAD");
                seen.add(item.get("id").asString());
            });
            cursor = page.get("nextCursor").isNull() ? null : page.get("nextCursor").asString();
            pages++;
        } while (cursor != null);

        assertThat(pages).isEqualTo(3);
        assertThat(seen).containsExactlyElementsOf(killedInOrder.reversed());
    }

    @Test
    void listCanBeFilteredByType() {
        UUID worker = registeredWorkerId();
        Job email = kill(worker, submit("email"));
        kill(worker, submit("sms"));

        JsonNode page = body(mvc.get().uri("/api/v1/dlq").param("type", "email").exchange());

        assertThat(page.get("items")).hasSize(1);
        assertThat(page.get("items").get(0).get("id").asString()).isEqualTo(email.id().toString());
        assertThat(page.get("items").get(0).get("lastError").asString()).isEqualTo("fatal");
    }

    @Test
    void requeueMakesTheJobPendingNowWithAllPastAttemptsExcused() {
        UUID worker = registeredWorkerId();
        Job dead = kill(worker, submit("t"));

        MvcTestResult result = mvc.post().uri("/api/v1/dlq/{id}/requeue", dead.id()).exchange();

        assertThat(result).hasStatusOk();
        JsonNode job = body(result);
        assertThat(job.get("status").asString()).isEqualTo("PENDING");
        assertThat(job.get("attempts").asInt()).isEqualTo(1);
        assertThat(job.get("attemptsExcused").asInt()).isEqualTo(1);
        assertThat(job.get("finishedAt").isNull()).isTrue();
        assertThat(job.get("lastError").asString()).isEqualTo("fatal");
        assertThat(body(mvc.get().uri("/api/v1/dlq").exchange()).get("items")).isEmpty();
    }

    /**
     * The regression test for attempt-number reuse: a requeued job continues its attempt
     * numbering (no UNIQUE (job_id, attempt) collision) and gets a complete new budget.
     */
    @Test
    void requeuedJobContinuesAttemptNumberingAndGetsAFullFreshBudget() {
        var runs = new AtomicInteger();
        Job job = submitWithMaxRetries("doomed", 2);
        startWorker(2, TestHandlers.of("doomed", ctx -> {
            runs.incrementAndGet();
            throw new IllegalStateException("fails on attempt " + ctx.attempt());
        }));
        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);
        assertThat(runs.get()).isEqualTo(3);

        assertThat(mvc.post().uri("/api/v1/dlq/{id}/requeue", job.id()).exchange()).hasStatusOk();

        // Three more attempts (4, 5, 6), not one and not zero, then dead again.
        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD && runs.get() == 6);
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).until(() -> runs.get() == 6);
        Job deadAgain = reload(job);
        assertThat(deadAgain.attempts()).isEqualTo(6);
        assertThat(deadAgain.attemptsExcused()).isEqualTo(3);
        assertThat(deadAgain.lastError()).contains("fails on attempt 6");
        assertThat(jobs.findAttempts(job.id())).extracting(JobAttempt::attempt).containsExactly(1, 2, 3, 4, 5, 6);
    }

    @Test
    void requeuedJobCanSucceed() {
        var fixed = new AtomicBoolean(false);
        Job job = submitWithMaxRetries("fixable", 0);
        startWorker(1, TestHandlers.of("fixable", ctx -> {
            if (!fixed.get()) {
                throw new IllegalStateException("downstream is broken");
            }
        }));
        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.DEAD);

        fixed.set(true);
        mvc.post().uri("/api/v1/dlq/{id}/requeue", job.id()).exchange();

        await().atMost(TIMEOUT).until(() -> reload(job).status() == JobStatus.SUCCEEDED);
        JsonNode detail = body(mvc.get().uri("/api/v1/jobs/{id}", job.id()).exchange());
        assertThat(detail.get("attemptHistory")).hasSize(2);
        assertThat(detail.get("attemptHistory").get(0).get("outcome").asString()).isEqualTo("FAILED");
        assertThat(detail.get("attemptHistory").get(1).get("attempt").asInt()).isEqualTo(2);
        assertThat(detail.get("attemptHistory").get(1).get("outcome").asString()).isEqualTo("SUCCEEDED");
    }

    @Test
    void requeueIsRefusedForJobsThatAreNotDeadAndUnknownJobs() {
        Job pending = submit("t");

        MvcTestResult notDead = mvc.post().uri("/api/v1/dlq/{id}/requeue", pending.id()).exchange();
        MvcTestResult unknown = mvc.post().uri("/api/v1/dlq/{id}/requeue", UUID.randomUUID()).exchange();

        assertThat(notDead).hasStatus(409).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(body(notDead).get("detail").asString()).contains("PENDING").contains("only DEAD");
        assertThat(unknown).hasStatus(404);
        assertThat(reload(pending).attemptsExcused()).isZero();
    }

    @Test
    void secondRequeueOfTheSameJobConflicts() {
        Job dead = kill(registeredWorkerId(), submit("t"));

        assertThat(mvc.post().uri("/api/v1/dlq/{id}/requeue", dead.id()).exchange()).hasStatusOk();
        assertThat(mvc.post().uri("/api/v1/dlq/{id}/requeue", dead.id()).exchange()).hasStatus(409);
    }

    /** Claims the job (the highest-priority due one) and fails it non-retryably. */
    private Job kill(UUID worker, Job job) {
        Job claimed = jobs.claim(worker, 1).getFirst();
        assertThat(claimed.id()).isEqualTo(job.id());
        assertThat(jobs.markFailed(claimed.id(), worker, claimed.attempts(), "fatal", false, Duration.ZERO))
                .contains(JobStatus.DEAD);
        return reload(job);
    }

    private JsonNode body(MvcTestResult result) {
        try {
            return json.readTree(result.getResponse().getContentAsString());
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
