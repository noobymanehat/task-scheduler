package dev.scheduler;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One real Postgres (same major version as compose.yaml) for the whole test run.
 *
 * <p>The container is a JVM-wide singleton rather than one per Spring context: the suite
 * uses several contexts (with and without MockMvc, with a live worker), and starting a
 * database for each is slow and gives container startup more chances to fail. Tests run
 * sequentially and every test empties the tables it uses, so sharing is safe.
 *
 * <p>It is deliberately not a {@code @ServiceConnection} bean: Spring Boot stops container
 * beans when their context closes, which would take the database away from every other
 * context. Only the connection settings are handed to Spring; Testcontainers removes the
 * container when the JVM exits.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"));

    static {
        POSTGRES.start();
    }

    @Bean
    DynamicPropertyRegistrar postgresProperties() {
        return registry -> {
            registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
            registry.add("spring.datasource.username", POSTGRES::getUsername);
            registry.add("spring.datasource.password", POSTGRES::getPassword);
        };
    }
}
