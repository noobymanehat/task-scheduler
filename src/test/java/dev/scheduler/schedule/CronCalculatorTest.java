package dev.scheduler.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class CronCalculatorTest {

    @Test
    void nextIsStrictlyAfterTheGivenInstant() {
        Instant onTheMinute = Instant.parse("2026-01-15T10:30:00Z");

        assertThat(CronCalculator.next("0 * * * * *", "UTC", onTheMinute))
                .contains(Instant.parse("2026-01-15T10:31:00Z"));
        assertThat(CronCalculator.next("0 * * * * *", "UTC", onTheMinute.minusMillis(1))).contains(onTheMinute);
        assertThat(CronCalculator.next("*/15 * * * * *", "UTC", Instant.parse("2026-01-15T10:30:07Z")))
                .contains(Instant.parse("2026-01-15T10:30:15Z"));
    }

    @Test
    void expressionIsEvaluatedInTheSchedulesTimezone() {
        Instant midnightUtc = Instant.parse("2026-01-15T00:00:00Z");
        String nineEveryMorning = "0 0 9 * * *";

        assertThat(CronCalculator.next(nineEveryMorning, "UTC", midnightUtc))
                .contains(Instant.parse("2026-01-15T09:00:00Z"));
        // 09:00 in Kolkata (UTC+5:30) is 03:30 UTC.
        assertThat(CronCalculator.next(nineEveryMorning, "Asia/Kolkata", midnightUtc))
                .contains(Instant.parse("2026-01-15T03:30:00Z"));
        // 09:00 in New York in winter (UTC-5) is 14:00 UTC.
        assertThat(CronCalculator.next(nineEveryMorning, "America/New_York", midnightUtc))
                .contains(Instant.parse("2026-01-15T14:00:00Z"));
    }

    @Test
    void localFireTimeIsKeptAcrossADaylightSavingChange() {
        String nineEveryMorning = "0 0 9 * * *";
        // US clocks go forward on 2026-03-08: New York moves from UTC-5 to UTC-4.
        Instant saturdayAfterNine = Instant.parse("2026-03-07T15:00:00Z");

        Instant sunday = CronCalculator.next(nineEveryMorning, "America/New_York", saturdayAfterNine).orElseThrow();
        Instant monday = CronCalculator.next(nineEveryMorning, "America/New_York", sunday).orElseThrow();

        assertThat(sunday).isEqualTo(Instant.parse("2026-03-08T13:00:00Z")); // 09:00 EDT
        assertThat(monday).isEqualTo(Instant.parse("2026-03-09T13:00:00Z"));
        // Only 23 hours separate Saturday's and Sunday's 09:00.
        assertThat(sunday).isEqualTo(Instant.parse("2026-03-07T14:00:00Z").plusSeconds(23 * 3600));
    }

    @Test
    void dayOfWeekAndMonthFieldsWork() {
        // 2026-01-15 is a Thursday.
        assertThat(CronCalculator.next("0 30 8 * * MON", "UTC", Instant.parse("2026-01-15T00:00:00Z")))
                .contains(Instant.parse("2026-01-19T08:30:00Z"));
        assertThat(CronCalculator.next("0 0 0 1 * *", "UTC", Instant.parse("2026-01-15T00:00:00Z")))
                .contains(Instant.parse("2026-02-01T00:00:00Z"));
    }

    @Test
    void validatesCronExpressionsAndTimezones() {
        assertThat(CronCalculator.isValidCron("0 */5 * * * *")).isTrue();
        assertThat(CronCalculator.isValidCron("@hourly")).isTrue();
        assertThat(CronCalculator.isValidCron("*/5 * * * *")).as("five-field Unix cron").isFalse();
        assertThat(CronCalculator.isValidCron("0 0 25 * * *")).as("hour 25").isFalse();
        assertThat(CronCalculator.isValidCron("every day")).isFalse();
        assertThat(CronCalculator.isValidCron(null)).isFalse();

        assertThat(CronCalculator.isValidTimezone("UTC")).isTrue();
        assertThat(CronCalculator.isValidTimezone("Europe/Berlin")).isTrue();
        assertThat(CronCalculator.isValidTimezone("Mars/Olympus_Mons")).isFalse();
        assertThat(CronCalculator.isValidTimezone("")).isFalse();
    }
}
