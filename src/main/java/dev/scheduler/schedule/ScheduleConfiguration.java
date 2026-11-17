package dev.scheduler.schedule;

import dev.scheduler.job.JobRepository;
import dev.scheduler.metrics.SchedulerMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ScheduleProperties.class)
public class ScheduleConfiguration {

    /** Runs wherever a worker runs, so schedules fire as long as any worker is up. */
    @Bean
    @ConditionalOnProperty(prefix = "scheduler.worker", name = "enabled", matchIfMissing = true)
    ScheduleFirer scheduleFirer(
            ScheduleRepository schedules,
            JobRepository jobs,
            TransactionTemplate tx,
            ScheduleProperties properties,
            SchedulerMetrics metrics) {
        return new ScheduleFirer(schedules, jobs, tx, properties.pollInterval(), metrics);
    }
}
