package dev.scheduler.worker;

import java.util.UUID;
import tools.jackson.databind.JsonNode;

/** What a handler is told about the job it is running. {@code attempt} starts at 1. */
public record JobContext(UUID jobId, String type, JsonNode payload, int attempt) {}
