package dev.scheduler.job;

import java.util.UUID;

public class JobNotCancellableException extends RuntimeException {

    public JobNotCancellableException(UUID id, JobStatus status) {
        super("Job " + id + " is " + status + "; only PENDING jobs can be cancelled");
    }
}
