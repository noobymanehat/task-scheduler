package dev.scheduler.admin;

import static org.assertj.core.api.Assertions.assertThat;

import dev.scheduler.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** The admin UI in an API process. {@link AdminUiDisabledTest} covers worker-only processes. */
@AutoConfigureMockMvc
class AdminUiTest extends IntegrationTest {

    @Autowired
    MockMvcTester mvc;

    @Test
    void rootForwardsToThePage() {
        assertThat(mvc.get().uri("/").exchange()).hasForwardedUrl("/ui/index.html");
    }

    @Test
    void pageAndAssetsAreServed() {
        assertThat(mvc.get().uri("/ui/index.html").exchange())
                .hasStatusOk()
                .bodyText()
                .contains("<title>Task Scheduler Admin</title>")
                .contains("Content-Security-Policy");
        assertThat(mvc.get().uri("/ui/app.js").exchange())
                .hasStatusOk()
                .hasContentTypeCompatibleWith("text/javascript");
        assertThat(mvc.get().uri("/ui/app.css").exchange())
                .hasStatusOk()
                .hasContentTypeCompatibleWith("text/css");
    }

    @Test
    void apiRoutesStillWinOverStaticFiles() {
        assertThat(mvc.get().uri("/api/v1/stats").exchange()).hasStatusOk();
    }
}
