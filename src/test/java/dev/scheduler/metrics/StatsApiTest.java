package dev.scheduler.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.scheduler.IntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureMockMvc
class StatsApiTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @BeforeEach
    void emptyWorkers() {
        jdbc.sql("DELETE FROM workers").update();
    }

    @Test
    void reportsActiveJobStatesOldestReadyAgeAndLiveWorkers() throws Exception {
        UUID alive = worker("ALIVE");
        worker("ALIVE");
        worker("DEAD");
        worker("STOPPED");
        job("PENDING", "now() - interval '90 seconds'", null);   // ready, oldest
        job("PENDING", "now() - interval '5 seconds'", null);    // ready
        job("PENDING", "now() + interval '1 hour'", null);       // scheduled (delayed or backing off)
        job("RUNNING", "now() - interval '1 minute'", alive);
        job("DEAD", "now() - interval '1 minute'", null);
        job("SUCCEEDED", "now() - interval '1 minute'", null);  // finished states are not counted
        job("CANCELLED", "now() - interval '1 minute'", null);

        JsonNode stats = get();

        assertThat(stats.get("ready").asLong()).isEqualTo(2);
        assertThat(stats.get("scheduled").asLong()).isEqualTo(1);
        assertThat(stats.get("running").asLong()).isEqualTo(1);
        assertThat(stats.get("dead").asLong()).isEqualTo(1);
        assertThat(stats.get("oldestReadySeconds").asDouble()).isCloseTo(90, within(5.0));
        assertThat(stats.get("workersAlive").asLong()).isEqualTo(2);
        assertThat(stats.get("heartbeatTimeoutSeconds").asDouble()).isEqualTo(30.0);
    }

    @Test
    void emptyQueueReportsZeros() throws Exception {
        JsonNode stats = get();

        assertThat(stats.get("ready").asLong()).isZero();
        assertThat(stats.get("oldestReadySeconds").asDouble()).isZero();
        assertThat(stats.get("workersAlive").asLong()).isZero();
    }

    /** The UI polls this every 2 s, so it must not serve the gauges' 5 s cached snapshot. */
    @Test
    void isNotCached() throws Exception {
        assertThat(get().get("ready").asLong()).isZero();
        job("PENDING", "now() - interval '1 second'", null);
        assertThat(get().get("ready").asLong()).isEqualTo(1);
    }

    private JsonNode get() throws Exception {
        var result = mvc.get().uri("/api/v1/stats").exchange();
        assertThat(result).hasStatusOk();
        return json.readTree(result.getResponse().getContentAsString());
    }

    private UUID worker(String status) {
        UUID id = UUID.randomUUID();
        jdbc.sql("INSERT INTO workers (id, hostname, status) VALUES (:id, 'test', :status)")
                .param("id", id)
                .param("status", status)
                .update();
        return id;
    }

    private void job(String status, String runAtSql, UUID lockedBy) {
        boolean running = status.equals("RUNNING");
        jdbc.sql("INSERT INTO jobs (type, status, run_at, attempts, locked_by, locked_at, finished_at) VALUES ('t', :status, "
                        + runAtSql + ", :attempts, :lockedBy, " + (running ? "now()" : "NULL") + ", "
                        + (status.equals("PENDING") || running ? "NULL" : "now()") + ")")
                .param("status", status)
                .param("attempts", running ? 1 : 0)
                .param("lockedBy", lockedBy)
                .update();
    }
}
