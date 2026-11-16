package dev.scheduler.job;

import java.time.Instant;
import java.util.UUID;

/** One execution of a job. {@code outcome} and {@code finishedAt} are null while it runs. */
public record JobAttempt(int attempt, UUID workerId, Instant startedAt, Instant finishedAt, String outcome, String error) {}
