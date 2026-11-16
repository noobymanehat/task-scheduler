package dev.scheduler.job;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/** A row of the {@code jobs} table. */
public record Job(
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
        UUID lockedBy,
        Instant lockedAt,
        String lastError,
        UUID scheduleId,
        Instant createdAt,
        Instant updatedAt,
        Instant finishedAt) {}
