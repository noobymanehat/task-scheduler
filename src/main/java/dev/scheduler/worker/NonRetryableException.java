package dev.scheduler.worker;

/**
 * Thrown by a handler to say that retrying cannot help (for example, the payload is
 * invalid). The job goes straight to the dead-letter queue regardless of retries left.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
