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
        assertThat(stopped).hasSize(1);
        assertThat(stopped.get(0).get("id").asString()).isEqualTo(stoppedId.toString());
        assertThat(stopped.get(0).get("runningJobs").asInt()).isZero();
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
