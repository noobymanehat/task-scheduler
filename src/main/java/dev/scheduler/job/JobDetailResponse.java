package dev.scheduler.job;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.List;

/** {@code GET /api/v1/jobs/{id}}: the job's fields plus its execution history, oldest first. */
public record JobDetailResponse(@JsonUnwrapped JobResponse job, List<JobAttempt> attemptHistory) {}
