package dev.scheduler.admin;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** Worker containers run the same image; they must serve neither the UI nor the API. */
@AutoConfigureMockMvc
@TestPropertySource(properties = "scheduler.api.enabled=false")
class AdminUiDisabledTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Test
    void nothingIsServedButActuator() {
        assertThat(mvc.get().uri("/").exchange()).hasStatus(404);
        assertThat(mvc.get().uri("/ui/index.html").exchange()).hasStatus(404);
        assertThat(mvc.get().uri("/api/v1/stats").exchange()).hasStatus(404);
        assertThat(mvc.get().uri("/actuator/health").exchange()).hasStatusOk();
    }
}
