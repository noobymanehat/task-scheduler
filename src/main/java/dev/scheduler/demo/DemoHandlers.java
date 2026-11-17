package dev.scheduler.demo;

import dev.scheduler.worker.JobContext;
import dev.scheduler.worker.JobHandler;
import dev.scheduler.worker.NonRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Handlers for trying the scheduler out (see README). They are also the reference for
 * writing a real one: implement {@link JobHandler} and expose it as a bean.
 *
 * <ul>
 *   <li>{@code demo.noop}: succeeds immediately.
 *   <li>{@code demo.sleep}: sleeps for payload {@code millis} (default 1000), then succeeds.
 *   <li>{@code demo.flaky}: fails until the attempt number reaches payload
 *       {@code succeedOnAttempt}; without it, fails every time and ends in the DLQ.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "scheduler.demo-handlers", name = "enabled", matchIfMissing = true)
public class DemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoHandlers.class);

    @Bean
    JobHandler demoNoopHandler() {
        return handler("demo.noop", context -> {});
    }

    @Bean
    JobHandler demoSleepHandler() {
        return handler("demo.sleep", context -> {
            long millis = context.payload().path("millis").asLong(1000);
            if (millis < 0) {
                throw new NonRetryableException("millis must not be negative: " + millis);
            }
            log.info("Job {} attempt {} sleeping {} ms", context.jobId(), context.attempt(), millis);
            Thread.sleep(millis); // interruptible, so timeouts and shutdown can stop it
        });
    }

    @Bean
    JobHandler demoFlakyHandler() {
        return handler("demo.flaky", context -> {
            int succeedOn = context.payload().path("succeedOnAttempt").asInt(Integer.MAX_VALUE);
            if (context.attempt() < succeedOn) {
                throw new IllegalStateException("demo.flaky failing on attempt " + context.attempt());
            }
        });
    }

    private interface Body {
        void run(JobContext context) throws Exception;
    }

    private static JobHandler handler(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(JobContext context) throws Exception {
                body.run(context);
            }
        };
    }
}
