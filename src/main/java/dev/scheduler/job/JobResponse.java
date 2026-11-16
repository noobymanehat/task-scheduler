package dev.scheduler.job;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public record JobResponse(
        UUID id,
        String type,
        JsonNode payload,
        int priority,
        JobStatus status,
        Instant runAt,
        int attempts,
        int attemptsExcused,
        int maxRetries,
        int timeoutSeconds,
        String idempotencyKey,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant finishedAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(
                job.id(),
                job.type(),
                job.payload(),
                job.priority(),
                job.status(),
                job.runAt(),
                job.attempts(),
                job.attemptsExcused(),
                job.maxRetries(),
                job.timeoutSeconds(),
                job.idempotencyKey(),
                job.lastError(),
                job.createdAt(),
                job.updatedAt(),
                job.finishedAt());
    }
}
