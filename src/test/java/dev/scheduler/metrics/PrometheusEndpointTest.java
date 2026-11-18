package dev.scheduler.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
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

    @Autowired
    MeterRegistry registry;

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
                .containsPattern("jobs_submitted_total\\{host=\"[^\"]+\",type=\"scrape-test\"} 4.0")
                .containsPattern("jobs_queue_depth\\{host=\"[^\"]+\",state=\"ready\"}")
                .containsPattern("jobs_queue_depth\\{host=\"[^\"]+\",state=\"scheduled\"}")
                .containsPattern("jobs_queue_depth\\{host=\"[^\"]+\",state=\"running\"}")
                .containsPattern("jobs_queue_depth\\{host=\"[^\"]+\",state=\"dead\"}")
                .contains("jobs_oldest_pending_age_seconds")
                .contains("workers_alive");
    }

    /**
     * Grafana computes queue-wait percentiles across all workers with histogram_quantile, which
     * needs the bucket series; client-side percentiles could not be combined.
     */
    @Test
    void latencyTimersPublishHistogramBuckets() {
        Timer.builder("job.queue.wait").tag("type", "scrape-test").register(registry).record(Duration.ofSeconds(90));
        Timer.builder("job.execution.duration")
                .tag("type", "scrape-test")
                .tag("outcome", "succeeded")
                .register(registry)
                .record(Duration.ofMillis(20));

        var scrape = mvc.get().uri("/actuator/prometheus").exchange();

        assertThat(scrape)
                .bodyText()
                .containsPattern("job_queue_wait_seconds_bucket\\{host=\"[^\"]+\",type=\"scrape-test\",le=\"[0-9.]+\"}")
                .contains("job_execution_duration_seconds_bucket{")
                // A 90 s wait must land in a finite bucket: the range extends past the 30 s default.
                .containsPattern("job_queue_wait_seconds_bucket\\{[^}]*type=\"scrape-test\",le=\"(1[0-9]{2}|[2-9][0-9]{2})\\.[0-9]+\"} 1");
    }

    @Test
    void healthEndpointIsExposed() {
        assertThat(mvc.get().uri("/actuator/health").exchange()).hasStatusOk().bodyText().contains("UP");
    }
}
