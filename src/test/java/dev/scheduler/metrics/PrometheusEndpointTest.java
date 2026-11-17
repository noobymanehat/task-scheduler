package dev.scheduler.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** The metrics as an operator sees them: scraped from the actuator endpoint. */
@AutoConfigureMockMvc
@AutoConfigureMetrics // tests disable metric exporters unless asked
class PrometheusEndpointTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Test
    void scrapeExposesSubmissionCountersAndQueueGauges() {
        for (int i = 0; i < 3; i++) {
            assertThat(mvc.post()
                            .uri("/api/v1/jobs")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"type\": \"scrape-test\"}")
                            .exchange())
                    .hasStatus(201);
        }
        // An idempotent replay is not a new submission.
        String keyed = "{\"type\": \"scrape-test\", \"idempotencyKey\": \"scrape-1\"}";
        mvc.post().uri("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(keyed).exchange();
        mvc.post().uri("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(keyed).exchange();

        var scrape = mvc.get().uri("/actuator/prometheus").exchange();

        assertThat(scrape).hasStatusOk();
        assertThat(scrape)
                .bodyText()
                .contains("jobs_submitted_total{type=\"scrape-test\"} 4.0")
                .contains("jobs_queue_depth{state=\"ready\"}")
                .contains("jobs_queue_depth{state=\"scheduled\"}")
                .contains("jobs_queue_depth{state=\"running\"}")
                .contains("jobs_queue_depth{state=\"dead\"}")
                .contains("jobs_oldest_pending_age_seconds")
                .contains("workers_alive");
    }

    @Test
    void healthEndpointIsExposed() {
        assertThat(mvc.get().uri("/actuator/health").exchange()).hasStatusOk().bodyText().contains("UP");
    }
}
