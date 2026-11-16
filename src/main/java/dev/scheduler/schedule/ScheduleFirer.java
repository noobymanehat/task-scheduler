package dev.scheduler.schedule;

import dev.scheduler.job.JobRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Turns due schedules into jobs. Every worker process runs one; there is no leader.
 *
 * <p>One pass is one transaction: lock the due schedules, insert a job for each, advance
 * each to its next fire time. Because the job insert and the advance commit together, a
 * tick can neither be lost (job missing, schedule advanced) nor repeated (job created,
 * schedule not advanced). Two guards keep concurrent firers to one job per tick: the row
 * lock, and an idempotency key derived from the schedule and its fire time.
 */
public class ScheduleFirer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ScheduleFirer.class);
    private static final int BATCH = 100;

    private final ScheduleRepository schedules;
    private final JobRepository jobs;
    private final TransactionTemplate tx;
    private final Duration interval;

    private volatile boolean running;
    private Thread thread;

    public ScheduleFirer(ScheduleRepository schedules, JobRepository jobs, TransactionTemplate tx, Duration interval) {
        this.schedules = schedules;
        this.jobs = jobs;
        this.tx = tx;
        this.interval = interval;
    }

    /** One pass over up to 100 due schedules. Returns how many jobs were created. */
    public int fireDue() {
        Integer fired = tx.execute(status -> {
            List<ScheduleRepository.Due> due = schedules.lockDue(BATCH);
            int created = 0;
            for (ScheduleRepository.Due item : due) {
                if (fire(item.schedule(), item.now())) {
                    created++;
                }
            }
            return created;
        });
        return fired == null ? 0 : fired;
    }

    private boolean fire(Schedule schedule, Instant now) {
        Optional<Instant> next;
        try {
            // Strictly after now, not after the missed tick: however many ticks were
            // missed during an outage, exactly one job is created and the rest are skipped.
            next = CronCalculator.next(schedule.cron(), schedule.timezone(), now);
        } catch (RuntimeException e) {
            // Only reachable if a row was edited by hand; do not let it block the others.
            log.error("Schedule {} ({}) has an unusable cron or timezone; disabling it",
                    schedule.id(), schedule.name(), e);
            schedules.disable(schedule.id());
            return false;
        }
        boolean created = jobs.insertForSchedule(
                schedule.id(),
                schedule.jobType(),
                schedule.payload().toString(),
                schedule.priority(),
                schedule.nextFireAt(),
                schedule.maxRetries(),
                schedule.timeoutSeconds(),
                tickKey(schedule));
        if (next.isPresent()) {
            schedules.advance(schedule.id(), next.get());
        } else {
            log.info("Schedule {} ({}) has no further fire times; disabling it", schedule.id(), schedule.name());
            schedules.advance(schedule.id(), schedule.nextFireAt());
            schedules.disable(schedule.id());
        }
        return created;
    }

    /** Identifies one tick of one schedule; reused as the job's idempotency key. */
    static String tickKey(Schedule schedule) {
        return "schedule:" + schedule.id() + ":" + schedule.nextFireAt().toEpochMilli();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("schedule-firer").daemon(true).start(this::loop);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        thread.interrupt();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        while (running) {
            try {
                int fired = fireDue();
                if (fired < BATCH) {
                    long base = interval.toMillis();
                    Thread.sleep(Math.max(1, base + ThreadLocalRandom.current().nextLong(base / 10 + 1)));
                }
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("Schedule firing pass failed; will retry", e);
                try {
                    Thread.sleep(Math.max(1, interval.toMillis()));
                } catch (InterruptedException interrupted) {
                    return;
                }
            }
        }
    }
}
