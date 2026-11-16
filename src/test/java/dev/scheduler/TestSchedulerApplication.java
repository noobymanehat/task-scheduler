package dev.scheduler;

import org.springframework.boot.SpringApplication;

/** Runs the app locally against a throwaway Testcontainers Postgres: {@code ./mvnw spring-boot:test-run}. */
public class TestSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.from(SchedulerApplication::main).with(TestcontainersConfiguration.class).run(args);
    }
}
