package dev.scheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.IntegrationTest;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** The application as deployed: a job submitted over HTTP is run by the Spring-managed worker. */
@AutoConfigureMockMvc
@TestPropertySource(properties = {"scheduler.worker.enabled=true", "scheduler.worker.poll-interval=20ms"})
@DirtiesContext
class WorkerEndToEndTest extends IntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Handlers {
        @Bean
        JobHandler greetHandler() {
            return TestHandlers.of("greet", ctx -> {});
        }
    }

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @Autowired
    Worker worker;

    @Test
    void submittedJobIsExecutedAndItsHistoryIsVisibleThroughTheApi() throws Exception {
        var submitted = mvc.post()
                .uri("/api/v1/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\": \"greet\", \"payload\": {\"name\": \"world\"}}")
                .exchange();
        assertThat(submitted).hasStatus(201);
        String id = json.readTree(submitted.getResponse().getContentAsString()).get("id").asString();

        await().atMost(Duration.ofSeconds(30)).until(() -> "SUCCEEDED".equals(fetch(id).get("status").asString()));

        JsonNode job = fetch(id);
        assertThat(job.get("attempts").asInt()).isEqualTo(1);
        assertThat(job.get("payload").get("name").asString()).isEqualTo("world");
        assertThat(job.get("finishedAt").isNull()).isFalse();
        assertThat(job.get("attemptHistory")).hasSize(1);
        JsonNode attempt = job.get("attemptHistory").get(0);
        assertThat(attempt.get("attempt").asInt()).isEqualTo(1);
        assertThat(attempt.get("outcome").asString()).isEqualTo("SUCCEEDED");
        assertThat(attempt.get("workerId").asString()).isEqualTo(worker.id().toString());
    }

    private JsonNode fetch(String id) throws Exception {
        var result = mvc.get().uri("/api/v1/jobs/{id}", id).exchange();
        assertThat(result).hasStatusOk();
        return json.readTree(result.getResponse().getContentAsString());
    }
}
