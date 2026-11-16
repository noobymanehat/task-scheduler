package dev.scheduler.worker;

/**
 * Executes jobs of one type. Register an implementation as a Spring bean.
 *
 * <p>Delivery is at-least-once: the same job may be handed to a handler more than once (for
 * example after a worker crash), so implementations must be idempotent. Returning normally
 * means success; throwing means the attempt failed.
 */
public interface JobHandler {

    /** The job {@code type} this handler executes. */
    String type();

    void handle(JobContext context) throws Exception;
}
