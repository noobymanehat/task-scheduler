package dev.scheduler.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureMockMvc
class ScheduleApiTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    JsonMapper json;

    @Test
    void createWithRequiredFieldsAppliesDefaultsAndComputesTheFirstFireTime() {
        Instant before = Instant.now();

        MvcTestResult result = post("""
                {"name": "cleanup", "cron": "0 * * * * *", "jobType": "cleanup.run"}
                """);

        assertThat(result).hasStatus(201);
        JsonNode schedule = body(result);
        assertThat(result.getResponse().getHeader("Location"))
                .isEqualTo("/api/v1/schedules/" + schedule.get("id").asString());
        assertThat(schedule.get("name").asString()).isEqualTo("cleanup");
        assertThat(schedule.get("timezone").asString()).isEqualTo("UTC");
        assertThat(schedule.get("enabled").asBoolean()).isTrue();
        assertThat(schedule.get("priority").asInt()).isZero();
        assertThat(schedule.get("maxRetries").asInt()).isEqualTo(3);
        assertThat(schedule.get("timeoutSeconds").asInt()).isEqualTo(300);
        assertThat(schedule.get("payload").isEmpty()).isTrue();
        assertThat(schedule.get("lastFiredAt").isNull()).isTrue();
        Instant next = Instant.parse(schedule.get("nextFireAt").asString());
        assertThat(next).isAfter(before.minusSeconds(30)).isBefore(before.plusSeconds(90));
        assertThat(next.getEpochSecond() % 60).isZero();
    }

    @Test
    void firstFireTimeHonoursTheTimezone() {
        JsonNode schedule = body(post("""
                {"name": "morning", "cron": "0 0 9 * * *", "timezone": "Asia/Kolkata", "jobType": "digest"}
                """));

        Instant next = Instant.parse(schedule.get("nextFireAt").asString());
        // 09:00 in Kolkata is 03:30 UTC all year (no daylight saving).
        assertThat(next.atZone(ZoneId.of("UTC")).getHour()).isEqualTo(3);
        assertThat(next.atZone(ZoneId.of("UTC")).getMinute()).isEqualTo(30);
        assertThat(next.atZone(ZoneId.of("Asia/Kolkata")).getHour()).isEqualTo(9);
    }

    @Test
    void invalidCronIsRejectedNamingTheField() {
        for (String cron : new String[] {"*/5 * * * *", "0 0 25 * * *", "whenever"}) {
            MvcTestResult result = post("""
                    {"name": "bad", "cron": "%s", "jobType": "t"}
                    """.formatted(cron));

            assertThat(result).as(cron).hasStatus(400).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
            assertThat(body(result).get("errors").get("cron").asString()).contains("6-field cron");
        }
        assertThat(body(mvc.get().uri("/api/v1/schedules").exchange())).isEmpty();
    }

    @Test
    void invalidTimezoneMissingFieldsAndOutOfRangeValuesAreRejected() {
        MvcTestResult result = post("""
                {"name": "has spaces", "cron": "", "timezone": "Mars/Phobos", "maxRetries": 99, "timeoutSeconds": 0}
                """);

        assertThat(result).hasStatus(400);
        assertThat(body(result).get("errors").propertyNames())
                .containsExactlyInAnyOrder("name", "cron", "timezone", "jobType", "maxRetries", "timeoutSeconds");
    }

    @Test
    void cronThatCanNeverFireIsRejected() {
        // Valid syntax, but there is no 30th of February.
        MvcTestResult result = post("""
                {"name": "never", "cron": "0 0 0 30 2 *", "jobType": "t"}
                """);

        assertThat(result).hasStatus(400);
        assertThat(body(result).get("detail").asString()).contains("never fires");
    }

    @Test
    void duplicateNameConflicts() {
        String request = """
                {"name": "unique-name", "cron": "0 * * * * *", "jobType": "t"}
                """;

        assertThat(post(request)).hasStatus(201);
        MvcTestResult second = post(request);

        assertThat(second).hasStatus(409).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(body(second).get("detail").asString()).contains("unique-name");
    }

    @Test
    void getListAndDelete() {
        String b = body(post("{\"name\": \"b\", \"cron\": \"0 * * * * *\", \"jobType\": \"t\"}")).get("id").asString();
        String a = body(post("{\"name\": \"a\", \"cron\": \"0 * * * * *\", \"jobType\": \"t\"}")).get("id").asString();

        assertThat(body(mvc.get().uri("/api/v1/schedules/{id}", a).exchange()).get("name").asString()).isEqualTo("a");
        JsonNode list = body(mvc.get().uri("/api/v1/schedules").exchange());
        assertThat(list).hasSize(2);
        assertThat(list.get(0).get("id").asString()).isEqualTo(a);
        assertThat(list.get(1).get("id").asString()).isEqualTo(b);

        assertThat(mvc.delete().uri("/api/v1/schedules/{id}", a).exchange()).hasStatus(204);
        assertThat(mvc.get().uri("/api/v1/schedules/{id}", a).exchange()).hasStatus(404);
        assertThat(mvc.delete().uri("/api/v1/schedules/{id}", a).exchange()).hasStatus(404);
        assertThat(body(mvc.get().uri("/api/v1/schedules").exchange())).hasSize(1);
    }

    @Test
    void pauseAndResumeAreIdempotent() {
        String id = body(post("{\"name\": \"p\", \"cron\": \"0 * * * * *\", \"jobType\": \"t\"}")).get("id").asString();

        MvcTestResult paused = mvc.post().uri("/api/v1/schedules/{id}/pause", id).exchange();
        MvcTestResult pausedAgain = mvc.post().uri("/api/v1/schedules/{id}/pause", id).exchange();
        assertThat(paused).hasStatusOk();
        assertThat(body(paused).get("enabled").asBoolean()).isFalse();
        assertThat(pausedAgain).hasStatusOk();
        assertThat(body(pausedAgain).get("enabled").asBoolean()).isFalse();

        MvcTestResult resumed = mvc.post().uri("/api/v1/schedules/{id}/resume", id).exchange();
        MvcTestResult resumedAgain = mvc.post().uri("/api/v1/schedules/{id}/resume", id).exchange();
        assertThat(resumed).hasStatusOk();
        assertThat(body(resumed).get("enabled").asBoolean()).isTrue();
        assertThat(resumedAgain).hasStatusOk();
        assertThat(body(resumedAgain).get("nextFireAt")).isEqualTo(body(resumed).get("nextFireAt"));
    }

    @Test
    void unknownScheduleIs404OnEveryRoute() {
        UUID id = UUID.randomUUID();

        assertThat(mvc.get().uri("/api/v1/schedules/{id}", id).exchange()).hasStatus(404);
        assertThat(mvc.delete().uri("/api/v1/schedules/{id}", id).exchange()).hasStatus(404);
        assertThat(mvc.post().uri("/api/v1/schedules/{id}/pause", id).exchange()).hasStatus(404);
        assertThat(mvc.post().uri("/api/v1/schedules/{id}/resume", id).exchange()).hasStatus(404);
    }

    private MvcTestResult post(String body) {
        return mvc.post().uri("/api/v1/schedules").contentType(MediaType.APPLICATION_JSON).content(body).exchange();
    }

    private JsonNode body(MvcTestResult result) {
        try {
            return json.readTree(result.getResponse().getContentAsString());
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
