package dev.scheduler.schedule;

import java.util.UUID;

public class ScheduleNotFoundException extends RuntimeException {

    public ScheduleNotFoundException(UUID id) {
        super("Schedule " + id + " not found");
    }
}
