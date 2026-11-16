package dev.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ScaffoldIntegrationTest extends IntegrationTest {

    @Test
    void contextLoadsAgainstRealPostgres16() {
        String version = jdbc.sql("SHOW server_version").query(String.class).single();

        assertThat(version).startsWith("16.");
    }

    @Test
    void flywayMigrationsAreAppliedSuccessfully() {
        var applied = jdbc.sql("SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank")
                .query(String.class)
                .list();
        long failed = jdbc.sql("SELECT count(*) FROM flyway_schema_history WHERE NOT success")
                .query(Long.class)
                .single();

        assertThat(applied).isNotEmpty().startsWith("1");
        assertThat(failed).isZero();
    }

    @Test
    void baselineMigrationRan() {
        String comment = jdbc.sql("SELECT obj_description('public'::regnamespace)").query(String.class).single();

        assertThat(comment).isEqualTo("task-scheduler");
    }
}
