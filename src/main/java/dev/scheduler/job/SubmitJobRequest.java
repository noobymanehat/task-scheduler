package dev.scheduler.job;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import tools.jackson.databind.JsonNode;

/** Body of {@code POST /api/v1/jobs}. Everything except {@code type} is optional. */
public record SubmitJobRequest(
        @NotBlank
                @Size(max = 100)
                @Pattern(regexp = "[A-Za-z0-9._-]+", message = "may contain only letters, digits, '.', '_' and '-'")
                String type,
        JsonNode payload,
        @Min(-1000) @Max(1000) Integer priority,
        Instant runAt,
        @Min(0) @Max(25) Integer maxRetries,
        @Min(1) @Max(86_400) Integer timeoutSeconds,
        @Size(min = 1, max = 255) String idempotencyKey) {}
