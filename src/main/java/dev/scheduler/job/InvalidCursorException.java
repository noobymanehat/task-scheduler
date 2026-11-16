package dev.scheduler.job;

public class InvalidCursorException extends RuntimeException {

    public InvalidCursorException() {
        super("Invalid cursor");
    }
}
