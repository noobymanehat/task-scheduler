package dev.scheduler.schedule;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** @param pollInterval how often each worker process looks for due schedules */
@ConfigurationProperties("scheduler.schedule")
public record ScheduleProperties(@DefaultValue("1s") Duration pollInterval) {}
