package dev.scheduler.schedule;

/** The request is well-formed but describes a schedule that cannot work. */
public class InvalidScheduleException extends RuntimeException {

    public InvalidScheduleException(String message) {
        super(message);
    }
}
