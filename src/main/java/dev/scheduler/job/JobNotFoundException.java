package dev.scheduler.job;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID id) {
        super("Job " + id + " not found");
    }
}
