package dev.scheduler.schedule;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/** A row of the {@code schedules} table. Also the API representation. */
public record Schedule(
        UUID id,
        String name,
        String cron,
        String timezone,
        String jobType,
        JsonNode payload,
        int priority,
        int maxRetries,
        int timeoutSeconds,
        boolean enabled,
        Instant nextFireAt,
        Instant lastFiredAt,
        Instant createdAt) {}
