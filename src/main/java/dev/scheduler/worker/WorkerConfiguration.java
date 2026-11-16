package dev.scheduler.worker;

import dev.scheduler.job.JobRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerConfiguration {

    @Bean
    HandlerRegistry handlerRegistry(ObjectProvider<JobHandler> handlers) {
        return new HandlerRegistry(handlers.orderedStream().toList());
    }

    /** Started and stopped with the application context via {@code SmartLifecycle}. */
    @Bean
    @ConditionalOnProperty(prefix = "scheduler.worker", name = "enabled", matchIfMissing = true)
    Worker worker(JobRepository jobs, WorkerRepository workers, HandlerRegistry handlers, WorkerProperties properties) {
        return new Worker(jobs, workers, handlers, properties);
    }

    @Bean
    @ConditionalOnProperty(prefix = "scheduler.worker", name = "enabled", matchIfMissing = true)
    Reaper reaper(WorkerRepository workers, JobRepository jobs, WorkerProperties properties) {
        return new Reaper(workers, jobs, properties.heartbeatTimeout(), properties.reaperInterval());
    }
}
