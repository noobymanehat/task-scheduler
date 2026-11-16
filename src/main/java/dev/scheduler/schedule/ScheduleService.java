package dev.scheduler.schedule;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class ScheduleService {

    static final int MAX_LISTED = 500;

    private final ScheduleRepository schedules;
    private final TransactionTemplate tx;

    public ScheduleService(ScheduleRepository schedules, TransactionTemplate tx) {
        this.schedules = schedules;
        this.tx = tx;
    }

    public Schedule create(CreateScheduleRequest r) {
        String timezone = r.timezone() == null ? "UTC" : r.timezone();
        Instant first = CronCalculator.next(r.cron(), timezone, schedules.now())
                .orElseThrow(() -> new InvalidScheduleException("cron expression '" + r.cron() + "' never fires"));
        try {
            return schedules.insert(
                    r.name(),
                    r.cron(),
                    timezone,
                    r.jobType(),
                    r.payload() == null || r.payload().isNull() ? "{}" : r.payload().toString(),
                    r.priority() == null ? 0 : r.priority(),
                    r.maxRetries() == null ? 3 : r.maxRetries(),
                    r.timeoutSeconds() == null ? 300 : r.timeoutSeconds(),
                    r.enabled() == null || r.enabled(),
                    first);
        } catch (DuplicateKeyException e) {
            throw new ScheduleConflictException("A schedule named '" + r.name() + "' already exists");
        }
    }

    public Schedule get(UUID id) {
        return schedules.findById(id).orElseThrow(() -> new ScheduleNotFoundException(id));
    }

    public List<Schedule> list() {
        return schedules.list(MAX_LISTED);
    }

    public void delete(UUID id) {
        if (!schedules.delete(id)) {
            throw new ScheduleNotFoundException(id);
        }
    }

    public Schedule pause(UUID id) {
        return schedules.pause(id).orElseThrow(() -> new ScheduleNotFoundException(id));
    }

    /**
     * Re-enables a schedule. The next fire time is computed from now, so ticks that passed
     * while it was paused are not made up. Resuming an enabled schedule changes nothing.
     */
    public Schedule resume(UUID id) {
        return tx.execute(status -> {
            var locked = schedules.lock(id).orElseThrow(() -> new ScheduleNotFoundException(id));
            Schedule schedule = locked.schedule();
            if (schedule.enabled()) {
                return schedule;
            }
            Instant next = CronCalculator.next(schedule.cron(), schedule.timezone(), locked.now())
                    .orElseThrow(() -> new ScheduleConflictException(
                            "Schedule " + id + " cannot be resumed: its cron expression never fires again"));
            return schedules.enable(id, next);
        });
    }
}
