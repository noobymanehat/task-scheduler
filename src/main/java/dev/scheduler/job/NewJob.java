package dev.scheduler.job;

import java.time.Instant;

/**
 * Values for a job insert. {@code runAt == null} means "now", evaluated by the database.
 * {@code payloadJson} is serialized JSON.
 */
public record NewJob(
        String type,
        String payloadJson,
        int priority,
        Instant runAt,
        int maxRetries,
        int timeoutSeconds,
        String idempotencyKey) {}
