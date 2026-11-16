package dev.scheduler.job;

import java.util.List;

/** One page of jobs, newest first. {@code nextCursor} is null on the last page. */
public record JobPage(List<JobResponse> items, String nextCursor) {}
