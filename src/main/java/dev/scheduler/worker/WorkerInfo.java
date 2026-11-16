package dev.scheduler.worker;

import java.time.Instant;
import java.util.UUID;

/** A row of {@code workers} plus how many jobs it currently holds. */
public record WorkerInfo(
        UUID id, String hostname, String status, Instant startedAt, Instant lastHeartbeatAt, long runningJobs) {}
