package dev.scheduler.job;

import java.util.UUID;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String key, UUID existingJobId) {
        super("Idempotency key '" + key + "' was already used by job " + existingJobId
                + " with different parameters");
    }
}
