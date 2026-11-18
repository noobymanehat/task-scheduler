package dev.scheduler.job;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/**
 * A job as the API returns it. {@code lockedBy} / {@code lockedAt} name the worker holding a
 * RUNNING job and when it claimed it; once the job leaves RUNNING they are null.
 */
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
        UUID lockedBy,
        Instant lockedAt,
        UUID scheduleId,
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
                job.lockedBy(),
                job.lockedAt(),
                job.scheduleId(),
                job.createdAt(),
                job.updatedAt(),
                job.finishedAt());
    }
}
