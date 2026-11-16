package dev.scheduler.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.scheduler.IntegrationTest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureMockMvc
class JobApiTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @BeforeEach
    void emptyJobs() {
        jdbc.sql("TRUNCATE jobs").update();
    }

    // ---- submit ----

    @Test
    void submitWithOnlyTypeAppliesDefaults() {
        Instant before = Instant.now();

        MvcTestResult result = post("""
                {"type": "send-email"}
                """);

        assertThat(result).hasStatus(201).hasContentTypeCompatibleWith(MediaType.APPLICATION_JSON);
        JsonNode job = body(result);
        assertThat(result.getResponse().getHeader("Location"))
                .isEqualTo("/api/v1/jobs/" + job.get("id").asString());
        assertThat(job.get("type").asString()).isEqualTo("send-email");
        assertThat(job.get("status").asString()).isEqualTo("PENDING");
        assertThat(job.get("payload").isObject()).isTrue();
        assertThat(job.get("payload").isEmpty()).isTrue();
        assertThat(job.get("priority").asInt()).isZero();
        assertThat(job.get("attempts").asInt()).isZero();
        assertThat(job.get("maxRetries").asInt()).isEqualTo(3);
        assertThat(job.get("timeoutSeconds").asInt()).isEqualTo(300);
        assertThat(job.get("idempotencyKey").isNull()).isTrue();
        assertThat(Instant.parse(job.get("runAt").asString())).isCloseTo(before, within(30, ChronoUnit.SECONDS));
    }

    @Test
    void submitStoresEveryFieldAndGetReturnsIt() {
        MvcTestResult result = post("""
                {
                  "type": "report.generate",
                  "payload": {"userId": 42, "tags": ["a", "b"], "nested": {"ok": true}},
                  "priority": 7,
                  "runAt": "2099-01-02T03:04:05.123456Z",
                  "maxRetries": 9,
                  "timeoutSeconds": 60,
                  "idempotencyKey": "report-42"
                }
                """);

        assertThat(result).hasStatus(201);
        String id = body(result).get("id").asString();

        MvcTestResult fetched = mvc.get().uri("/api/v1/jobs/{id}", id).exchange();
        assertThat(fetched).hasStatusOk();
        JsonNode job = body(fetched);
        assertThat(job.get("payload")).isEqualTo(json.readTree("""
                {"userId": 42, "tags": ["a", "b"], "nested": {"ok": true}}
                """));
        assertThat(job.get("priority").asInt()).isEqualTo(7);
        assertThat(Instant.parse(job.get("runAt").asString()))
                .isEqualTo(Instant.parse("2099-01-02T03:04:05.123456Z"));
        assertThat(job.get("maxRetries").asInt()).isEqualTo(9);
        assertThat(job.get("timeoutSeconds").asInt()).isEqualTo(60);
        assertThat(job.get("idempotencyKey").asString()).isEqualTo("report-42");
    }

    @Test
    void submitsWithoutIdempotencyKeyAlwaysCreateNewJobs() {
        String first = body(post("{\"type\": \"t\"}")).get("id").asString();
        String second = body(post("{\"type\": \"t\"}")).get("id").asString();

        assertThat(first).isNotEqualTo(second);
        assertThat(jobCount()).isEqualTo(2);
    }

    // ---- idempotency ----

    @Test
    void replayWithSameKeyAndBodyReturnsOriginalJobWithoutCreatingAnother() {
        String request = """
                {"type": "charge", "payload": {"orderId": 1, "amount": 10}, "priority": 5, "idempotencyKey": "order-1"}
                """;
        // Same request, but with keys reordered and different whitespace.
        String equivalent = """
                {"idempotencyKey":"order-1","priority":5,"payload":{"amount":10,"orderId":1},"type":"charge"}
                """;

        MvcTestResult first = post(request);
        MvcTestResult second = post(request);
        MvcTestResult third = post(equivalent);

        assertThat(first).hasStatus(201);
        assertThat(second).hasStatus(200);
        assertThat(third).hasStatus(200);
        String id = body(first).get("id").asString();
        assertThat(body(second).get("id").asString()).isEqualTo(id);
        assertThat(body(third).get("id").asString()).isEqualTo(id);
        assertThat(jobCount()).isEqualTo(1);
    }

    @Test
    void replayWithSameKeyButDifferentBodyIsRejected() {
        MvcTestResult first = post("""
                {"type": "charge", "payload": {"amount": 10}, "idempotencyKey": "order-2"}
                """);
        MvcTestResult differentPayload = post("""
                {"type": "charge", "payload": {"amount": 9999}, "idempotencyKey": "order-2"}
                """);
        MvcTestResult differentRunAt = post("""
                {"type": "charge", "payload": {"amount": 10}, "runAt": "2099-01-01T00:00:00Z", "idempotencyKey": "order-2"}
                """);

        assertThat(first).hasStatus(201);
        assertThat(differentPayload).hasStatus(409).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(body(differentPayload).get("detail").asString())
                .contains("order-2")
                .contains(body(first).get("id").asString());
        assertThat(differentRunAt).hasStatus(409);
        assertThat(jobCount()).isEqualTo(1);
    }

    @Test
    void replayWithExplicitRunAtMatchesOnlyTheSameInstant() {
        String request = """
                {"type": "t", "runAt": "2099-06-01T12:00:00.123456789Z", "idempotencyKey": "k-runat"}
                """;

        assertThat(post(request)).hasStatus(201);
        // Nanoseconds are truncated to the database's microsecond precision on both sides.
        assertThat(post(request)).hasStatus(200);
    }

    // ---- validation ----

    @Test
    void invalidFieldsAreRejectedWithProblemDetailNamingEachField() {
        MvcTestResult result = post("""
                {"type": " ", "priority": 5000, "maxRetries": -1, "timeoutSeconds": 0, "idempotencyKey": ""}
                """);

        assertThat(result).hasStatus(400).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        JsonNode errors = body(result).get("errors");
        assertThat(errors.propertyNames())
                .containsExactlyInAnyOrder("type", "priority", "maxRetries", "timeoutSeconds", "idempotencyKey");
        assertThat(jobCount()).isZero();
    }

    @Test
    void missingTypeMalformedJsonAndBadTimestampAreRejected() {
        assertThat(post("{}")).hasStatus(400);
        assertThat(post("{\"type\": \"t\", ")).hasStatus(400);
        assertThat(post("{\"type\": \"t\", \"runAt\": \"tomorrow\"}")).hasStatus(400);
        assertThat(post("{\"type\": \"has spaces\"}")).hasStatus(400);
        assertThat(jobCount()).isZero();
    }

    // ---- get ----

    @Test
    void getUnknownJobIs404AndMalformedIdIs400() {
        MvcTestResult unknown = mvc.get().uri("/api/v1/jobs/{id}", UUID.randomUUID()).exchange();
        MvcTestResult malformed = mvc.get().uri("/api/v1/jobs/not-a-uuid").exchange();

        assertThat(unknown).hasStatus(404).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(malformed).hasStatus(400);
    }

    // ---- list ----

    @Test
    void listPagesThroughAllJobsNewestFirstWithoutDuplicates() {
        List<String> submitted = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            submitted.add(body(post("{\"type\": \"t\"}")).get("id").asString());
        }

        List<String> seen = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            var request = mvc.get().uri("/api/v1/jobs").param("limit", "2");
            if (cursor != null) {
                request = request.param("cursor", cursor);
            }
            JsonNode page = body(request.exchange());
            page.get("items").forEach(item -> seen.add(item.get("id").asString()));
            cursor = page.get("nextCursor").isNull() ? null : page.get("nextCursor").asString();
            pages++;
        } while (cursor != null);

        assertThat(pages).isEqualTo(3);
        assertThat(seen).containsExactlyElementsOf(submitted.reversed());
    }

    @Test
    void listFiltersByStatusAndType() {
        String emailId = body(post("{\"type\": \"email\"}")).get("id").asString();
        String cancelledId = body(post("{\"type\": \"email\"}")).get("id").asString();
        post("{\"type\": \"sms\"}");
        mvc.post().uri("/api/v1/jobs/{id}/cancel", cancelledId).exchange();

        JsonNode pendingEmails = body(mvc.get()
                .uri("/api/v1/jobs")
                .param("status", "PENDING")
                .param("type", "email")
                .exchange());
        JsonNode cancelled = body(mvc.get().uri("/api/v1/jobs").param("status", "CANCELLED").exchange());

        assertThat(ids(pendingEmails)).containsExactly(emailId);
        assertThat(ids(cancelled)).containsExactly(cancelledId);
        assertThat(pendingEmails.get("nextCursor").isNull()).isTrue();
    }

    @Test
    void listRejectsUnknownStatusAndInvalidCursor() {
        assertThat(mvc.get().uri("/api/v1/jobs").param("status", "BOGUS").exchange()).hasStatus(400);
        assertThat(mvc.get().uri("/api/v1/jobs").param("cursor", "!!!not-base64!!!").exchange()).hasStatus(400);
        assertThat(mvc.get().uri("/api/v1/jobs").param("cursor", "aGVsbG8").exchange()).hasStatus(400);
    }

    // ---- cancel ----

    @Test
    void cancelPendingJobThenSecondCancelConflicts() {
        String id = body(post("{\"type\": \"t\"}")).get("id").asString();

        MvcTestResult first = mvc.post().uri("/api/v1/jobs/{id}/cancel", id).exchange();
        MvcTestResult second = mvc.post().uri("/api/v1/jobs/{id}/cancel", id).exchange();

        assertThat(first).hasStatusOk();
        assertThat(body(first).get("status").asString()).isEqualTo("CANCELLED");
        assertThat(body(first).get("finishedAt").isNull()).isFalse();
        assertThat(second).hasStatus(409).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(body(second).get("detail").asString()).contains("CANCELLED");
    }

    @Test
    void cancelIsRefusedForEveryNonPendingStatus() {
        for (String status : List.of("RUNNING", "SUCCEEDED", "DEAD")) {
            String id = body(post("{\"type\": \"t\"}")).get("id").asString();
            jdbc.sql("UPDATE jobs SET status = :s WHERE id = CAST(:id AS uuid)")
                    .param("s", status)
                    .param("id", id)
                    .update();

            assertThat(mvc.post().uri("/api/v1/jobs/{id}/cancel", id).exchange()).hasStatus(409);
            assertThat(jdbc.sql("SELECT status FROM jobs WHERE id = CAST(:id AS uuid)")
                            .param("id", id)
                            .query(String.class)
                            .single())
                    .isEqualTo(status);
        }
    }

    @Test
    void cancelUnknownJobIs404() {
        assertThat(mvc.post().uri("/api/v1/jobs/{id}/cancel", UUID.randomUUID()).exchange()).hasStatus(404);
    }

    // ---- helpers ----

    private MvcTestResult post(String body) {
        return mvc.post().uri("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(body).exchange();
    }

    private JsonNode body(MvcTestResult result) {
        try {
            return json.readTree(result.getResponse().getContentAsString());
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> ids(JsonNode page) {
        List<String> ids = new ArrayList<>();
        page.get("items").forEach(item -> ids.add(item.get("id").asString()));
        return ids;
    }

    private long jobCount() {
        return jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single();
    }
}
