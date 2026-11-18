package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureMockMvc
class WorkersApiTest extends WorkerTestSupport {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @Test
    void listsWorkersWithStatusHeartbeatAndRunningJobCount() throws Exception {
        var started = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        submit("work");
        Worker busy = startWorker(2, TestHandlers.of("work", ctx -> {
            started.countDown();
            finish.await();
        }));
        assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
        Worker idle = startWorker(1);
        UUID stoppedId = idle.id();
        idle.stop();

        JsonNode all = get("/api/v1/workers");
        JsonNode alive = get("/api/v1/workers?status=ALIVE");
        JsonNode stopped = get("/api/v1/workers?status=STOPPED");
        finish.countDown();

        assertThat(all).hasSize(2);
        assertThat(alive).hasSize(1);
        JsonNode worker = alive.get(0);
        assertThat(worker.get("id").asString()).isEqualTo(busy.id().toString());
        assertThat(worker.get("status").asString()).isEqualTo("ALIVE");
        assertThat(worker.get("hostname").asString()).isNotBlank();
        assertThat(worker.get("runningJobs").asInt()).isEqualTo(1);
        assertThat(worker.get("startedAt").isNull()).isFalse();
        assertThat(worker.get("lastHeartbeatAt").isNull()).isFalse();
        // Heartbeats every 20 ms in tests, so the age is small and never negative.
        assertThat(worker.get("heartbeatAgeSeconds").asDouble()).isBetween(0.0, 5.0);
        assertThat(stopped).hasSize(1);
        assertThat(stopped.get(0).get("id").asString()).isEqualTo(stoppedId.toString());
        assertThat(stopped.get(0).get("runningJobs").asInt()).isZero();
    }

    /** Computed with the database clock, so a client never compares against its own. */
    @Test
    void heartbeatAgeIsMeasuredByTheDatabase() throws Exception {
        UUID id = registeredWorkerId();
        expireHeartbeat(id);

        JsonNode worker = get("/api/v1/workers").get(0);

        assertThat(worker.get("id").asString()).isEqualTo(id.toString());
        assertThat(worker.get("heartbeatAgeSeconds").asDouble()).isBetween(3600.0, 3660.0);
    }

    /** The UI shows which worker holds a running job; that link must clear when the job finishes. */
    @Test
    void jobResponsesNameTheWorkerHoldingARunningJob() throws Exception {
        UUID owner = registeredWorkerId();
        var job = submit("t");
        var claimed = jobs.claim(owner, 1).getFirst();

        JsonNode running = get("/api/v1/jobs/" + job.id());
        assertThat(running.get("status").asString()).isEqualTo("RUNNING");
        assertThat(running.get("lockedBy").asString()).isEqualTo(owner.toString());
        assertThat(running.get("lockedAt").isNull()).isFalse();
        assertThat(get("/api/v1/jobs?status=RUNNING").get("items").get(0).get("lockedBy").asString())
                .isEqualTo(owner.toString());

        assertThat(jobs.markSucceeded(job.id(), owner, claimed.attempts())).isTrue();

        JsonNode done = get("/api/v1/jobs/" + job.id());
        assertThat(done.get("lockedBy").isNull()).isTrue();
        assertThat(done.get("attemptHistory").get(0).get("workerId").asString()).isEqualTo(owner.toString());
    }

    @Test
    void unknownStatusFilterIsRejected() {
        assertThat(mvc.get().uri("/api/v1/workers").param("status", "ZOMBIE").exchange()).hasStatus(400);
    }

    private JsonNode get(String uri) throws Exception {
        var result = mvc.get().uri(uri).exchange();
        assertThat(result).hasStatusOk();
        return json.readTree(result.getResponse().getContentAsString());
    }
}
