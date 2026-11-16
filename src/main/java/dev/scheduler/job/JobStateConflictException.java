package dev.scheduler.job;

import java.util.UUID;

/** The job exists but is not in the status the requested transition starts from. */
public class JobStateConflictException extends RuntimeException {

    public JobStateConflictException(UUID id, JobStatus actual, String requirement) {
        super("Job " + id + " is " + actual + "; " + requirement);
    }
}
