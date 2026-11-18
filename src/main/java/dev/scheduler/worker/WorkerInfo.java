package dev.scheduler.worker;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of {@code workers} plus how many jobs it currently holds. {@code heartbeatAgeSeconds}
 * is computed with the database clock, so clients need not compare timestamps against their own.
 */
public record WorkerInfo(
        UUID id,
        String hostname,
        String status,
        Instant startedAt,
        Instant lastHeartbeatAt,
        double heartbeatAgeSeconds,
        long runningJobs) {}
