package dev.scheduler.job;

/** Job states; transitions are defined in DESIGN.md §4. */
public enum JobStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    DEAD,
    CANCELLED
}
