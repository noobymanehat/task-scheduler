package dev.scheduler.worker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Looks up the handler for a job type. Two handlers claiming one type is a startup error. */
public class HandlerRegistry {

    private final Map<String, JobHandler> byType = new HashMap<>();

    public HandlerRegistry(Collection<JobHandler> handlers) {
        for (JobHandler handler : handlers) {
            JobHandler previous = byType.putIfAbsent(handler.type(), handler);
            if (previous != null) {
                throw new IllegalStateException("Two handlers registered for job type '" + handler.type() + "': "
                        + previous.getClass().getName() + " and " + handler.getClass().getName());
            }
        }
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(byType.get(type));
    }
}
