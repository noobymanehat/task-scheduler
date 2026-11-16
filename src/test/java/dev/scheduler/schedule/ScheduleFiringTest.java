package dev.scheduler.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.scheduler.IntegrationTest;
import dev.scheduler.job.Job;
import dev.scheduler.job.JobRepository;
import dev.scheduler.job.JobStatus;
import dev.scheduler.job.NewJob;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

class ScheduleFiringTest extends IntegrationTest {

    private static final String EVERY_MINUTE = "0 * * * * *";
    private static final String EVERY_SECOND = "* * * * * *";

    @Autowired
    ScheduleService service;

    @Autowired
    ScheduleRepository schedules;

    @Autowired
    JobRepository jobs;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    JsonMapper json;

    private final List<ScheduleFirer> started = new ArrayList<>();

    @AfterEach
    void stopFirers() {
        started.forEach(ScheduleFirer::stop);
    }

    @Test
    void scheduleThatIsNotYetDueDoesNotFire() {
        Schedule schedule = create("not-due", EVERY_MINUTE);

        assertThat(firer().fireDue()).isZero();

        assertThat(jobsOf(schedule)).isEmpty();
        assertThat(reload(schedule).lastFiredAt()).isNull();
        assertThat(reload(schedule).nextFireAt()).isEqualTo(schedule.nextFireAt());
    }

    @Test
    void dueScheduleCreatesOneJobFromItsTemplateAndAdvances() {
        Schedule schedule = service.create(new CreateScheduleRequest(
                "nightly-report",
                EVERY_MINUTE,
                "UTC",
                "report.generate",
                json.readTree("{\"format\": \"pdf\"}"),
                7,
                5,
                120,
                true));
        Instant tick = makeDue(schedule, Duration.ofSeconds(5));

        int fired = firer().fireDue();

        assertThat(fired).isEqualTo(1);
        assertThat(jobsOf(schedule)).singleElement().satisfies(job -> {
            assertThat(job.type()).isEqualTo("report.generate");
            assertThat(job.payload().get("format").asString()).isEqualTo("pdf");
            assertThat(job.priority()).isEqualTo(7);
            assertThat(job.maxRetries()).isEqualTo(5);
            assertThat(job.timeoutSeconds()).isEqualTo(120);
            assertThat(job.status()).isEqualTo(JobStatus.PENDING);
            assertThat(job.scheduleId()).isEqualTo(schedule.id());
            // The job is stamped with the tick it belongs to, not the time the firer got to it.
            assertThat(job.runAt()).isEqualTo(tick);
            assertThat(job.idempotencyKey()).isEqualTo("schedule:" + schedule.id() + ":" + tick.toEpochMilli());
        });
        Schedule advanced = reload(schedule);
        assertThat(advanced.lastFiredAt()).isNotNull();
        assertThat(advanced.nextFireAt()).isAfter(schedules.now().minusSeconds(1)).isAfter(tick);
        assertThat(advanced.nextFireAt().getEpochSecond() % 60).isZero();
    }

    /** After an hour of downtime an every-minute schedule produces one job, not sixty. */
    @Test
    void missedTicksFireOnceThenSkipToTheNextFutureTick() {
        Schedule schedule = create("misfire", EVERY_MINUTE);
        makeDue(schedule, Duration.ofHours(1));
        Instant before = schedules.now();

        assertThat(firer().fireDue()).isEqualTo(1);

        assertThat(jobsOf(schedule)).hasSize(1);
        Instant next = reload(schedule).nextFireAt();
        assertThat(next).isAfter(before).isBeforeOrEqualTo(before.plusSeconds(61));
    }

    /** The headline guarantee for cron: any number of instances, one job per tick. */
    @Test
    void fourConcurrentFirersCreateExactlyOneJobPerScheduleTick() throws Exception {
        int scheduleCount = 20;
        int firers = 4;
        int rounds = 5;
        List<Schedule> all = new ArrayList<>();
        for (int i = 0; i < scheduleCount; i++) {
            all.add(create("concurrent-" + i, EVERY_MINUTE));
        }

        for (int round = 1; round <= rounds; round++) {
            // A different tick each round, so each round has its own idempotency keys.
            jdbc.sql("UPDATE schedules SET next_fire_at = now() - :hours * interval '1 hour'")
                    .param("hours", round)
                    .update();
            var go = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>();
            try (ExecutorService pool = Executors.newFixedThreadPool(firers)) {
                for (int i = 0; i < firers; i++) {
                    ScheduleFirer firer = firer();
                    Callable<Integer> task = () -> {
                        go.await();
                        // Keep going until this firer sees nothing due.
                        int total = 0;
                        int fired;
                        do {
                            fired = firer.fireDue();
                            total += fired;
                        } while (fired > 0);
                        return total;
                    };
                    futures.add(pool.submit(task));
                }
                go.countDown();
            }
            int firedThisRound = 0;
            for (var future : futures) {
                firedThisRound += future.get();
            }
            // SKIP LOCKED can leave a row for a later pass; sweep once more before counting.
            firedThisRound += firer().fireDue();
            assertThat(firedThisRound).as("jobs created in round %d", round).isEqualTo(scheduleCount);
        }

        assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single())
                .isEqualTo((long) scheduleCount * rounds);
        for (Schedule schedule : all) {
            assertThat(jobsOf(schedule)).extracting(Job::runAt).doesNotHaveDuplicates().hasSize(rounds);
        }
    }

    /** The second, independent guard: even past the row lock, a tick's key admits one job. */
    @Test
    void tickWhoseJobAlreadyExistsCreatesNoSecondJobButStillAdvances() {
        Schedule schedule = create("already-fired", EVERY_MINUTE);
        makeDue(schedule, Duration.ofMinutes(5));
        Schedule due = reload(schedule);
        jobs.insert(new NewJob("t", "{}", 0, null, 3, 300, ScheduleFirer.tickKey(due))).orElseThrow();

        int fired = firer().fireDue();

        assertThat(fired).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single()).isEqualTo(1);
        assertThat(reload(schedule).nextFireAt()).isAfter(due.nextFireAt());
    }

    @Test
    void pausedScheduleDoesNotFireAndResumeDoesNotCatchUp() {
        Schedule schedule = create("pausable", EVERY_MINUTE);
        service.pause(schedule.id());
        makeDue(schedule, Duration.ofHours(2));

        assertThat(firer().fireDue()).isZero();
        assertThat(jobsOf(schedule)).isEmpty();

        Instant before = schedules.now();
        Schedule resumed = service.resume(schedule.id());

        assertThat(resumed.enabled()).isTrue();
        assertThat(resumed.nextFireAt()).isAfter(before);
        // The two hours of ticks missed while paused are not made up.
        assertThat(firer().fireDue()).isZero();
        assertThat(jobsOf(schedule)).isEmpty();
    }

    @Test
    void resumingAnEnabledScheduleLeavesItsNextFireTimeAlone() {
        Schedule schedule = create("already-enabled", EVERY_MINUTE);
        Instant tick = makeDue(schedule, Duration.ofMinutes(3));

        Schedule resumed = service.resume(schedule.id());

        assertThat(resumed.nextFireAt()).isEqualTo(tick);
        assertThat(firer().fireDue()).isEqualTo(1);
    }

    @Test
    void scheduleCreatedDisabledDoesNotFireUntilResumed() {
        Schedule schedule = service.create(
                new CreateScheduleRequest("starts-off", EVERY_MINUTE, null, "t", null, null, null, null, false));
        makeDue(schedule, Duration.ofMinutes(1));

        assertThat(schedule.enabled()).isFalse();
        assertThat(firer().fireDue()).isZero();
    }

    @Test
    void deletingAScheduleKeepsTheJobsItCreated() {
        Schedule schedule = create("short-lived", EVERY_MINUTE);
        makeDue(schedule, Duration.ofMinutes(1));
        firer().fireDue();
        UUID jobId = jobsOf(schedule).getFirst().id();

        service.delete(schedule.id());

        Job orphan = jobs.findById(jobId).orElseThrow();
        assertThat(orphan.scheduleId()).isNull();
        assertThat(orphan.status()).isEqualTo(JobStatus.PENDING);
        assertThat(firer().fireDue()).isZero();
    }

    @Test
    void rowWithUnusableCronIsDisabledWithoutBlockingOtherSchedules() {
        Schedule broken = create("broken", EVERY_MINUTE);
        Schedule healthy = create("healthy", EVERY_MINUTE);
        jdbc.sql("UPDATE schedules SET cron = 'not a cron' WHERE id = :id").param("id", broken.id()).update();
        makeDue(broken, Duration.ofMinutes(2));
        makeDue(healthy, Duration.ofMinutes(1));

        assertThat(firer().fireDue()).isEqualTo(1);

        assertThat(reload(broken).enabled()).isFalse();
        assertThat(jobsOf(broken)).isEmpty();
        assertThat(jobsOf(healthy)).hasSize(1);
    }

    /** The real loop, on two instances at once, against an every-second schedule. */
    @Test
    void runningFirersFireAnEverySecondScheduleOncePerSecond() {
        Schedule schedule = create("ticker", EVERY_SECOND);
        for (int i = 0; i < 2; i++) {
            var firer = new ScheduleFirer(schedules, jobs, tx, Duration.ofMillis(20));
            firer.start();
            started.add(firer);
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> jobsOf(schedule).size() >= 3);

        List<Job> fired = jobsOf(schedule);
        assertThat(fired).extracting(Job::runAt).doesNotHaveDuplicates();
        assertThat(fired).allSatisfy(job -> assertThat(job.runAt().getNano()).isZero());
        // Consecutive ticks: no second was skipped and none was doubled.
        List<Long> seconds = fired.stream().map(j -> j.runAt().getEpochSecond()).sorted().toList();
        for (int i = 1; i < seconds.size(); i++) {
            assertThat(seconds.get(i) - seconds.get(i - 1)).isEqualTo(1);
        }
    }

    // ---- helpers ----

    private ScheduleFirer firer() {
        return new ScheduleFirer(schedules, jobs, tx, Duration.ofHours(1));
    }

    private Schedule create(String name, String cron) {
        return service.create(new CreateScheduleRequest(name, cron, null, "t", null, null, null, null, null));
    }

    /** Moves the schedule's next fire time into the past and returns it. */
    private Instant makeDue(Schedule schedule, Duration ago) {
        jdbc.sql("UPDATE schedules SET next_fire_at = date_trunc('second', now()) - :s * interval '1 second' WHERE id = :id")
                .param("s", ago.toSeconds())
                .param("id", schedule.id())
                .update();
        return reload(schedule).nextFireAt();
    }

    private Schedule reload(Schedule schedule) {
        return schedules.findById(schedule.id()).orElseThrow();
    }

    private List<Job> jobsOf(Schedule schedule) {
        List<UUID> ids = jdbc.sql("SELECT id FROM jobs WHERE schedule_id = :id ORDER BY run_at")
                .param("id", schedule.id())
                .query(UUID.class)
                .list();
        return ids.stream().map(id -> jobs.findById(id).orElseThrow()).toList();
    }
}
