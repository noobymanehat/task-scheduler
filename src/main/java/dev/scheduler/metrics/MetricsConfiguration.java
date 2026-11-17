package dev.scheduler.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

@Configuration(proxyBeanMethods = false)
public class MetricsConfiguration {

    @Bean
    QueueStats queueStats(
            JdbcClient jdbc,
            MeterRegistry registry,
            @Value("${scheduler.metrics.refresh-interval:5s}") Duration refreshInterval) {
        return new QueueStats(jdbc, registry, refreshInterval);
    }
}
