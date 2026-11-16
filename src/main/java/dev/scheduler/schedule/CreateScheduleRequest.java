package dev.scheduler.schedule;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

/**
 * Body of {@code POST /api/v1/schedules}. {@code name}, {@code cron} and {@code jobType} are
 * required; the job fields have the same limits and defaults as a directly submitted job.
 */
public record CreateScheduleRequest(
        @NotBlank
                @Size(max = 100)
                @Pattern(regexp = "[A-Za-z0-9._-]+", message = "may contain only letters, digits, '.', '_' and '-'")
                String name,
        @NotBlank @ValidCron String cron,
        @ValidTimezone String timezone,
        @NotBlank
                @Size(max = 100)
                @Pattern(regexp = "[A-Za-z0-9._-]+", message = "may contain only letters, digits, '.', '_' and '-'")
                String jobType,
        JsonNode payload,
        @Min(-1000) @Max(1000) Integer priority,
        @Min(0) @Max(25) Integer maxRetries,
        @Min(1) @Max(86_400) Integer timeoutSeconds,
        Boolean enabled) {}
