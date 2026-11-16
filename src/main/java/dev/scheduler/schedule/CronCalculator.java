package dev.scheduler.schedule;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.springframework.scheduling.support.CronExpression;

/**
 * Cron arithmetic. Expressions use Spring's six fields (second minute hour day-of-month
 * month day-of-week) and are evaluated in the schedule's own timezone, so "every day at
 * 09:00" stays at 09:00 local time across daylight-saving changes.
 */
public final class CronCalculator {

    private CronCalculator() {}

    /** The first fire time strictly after {@code after}, or empty if the expression never fires again. */
    public static Optional<Instant> next(String cron, String timezone, Instant after) {
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZone(ZoneId.of(timezone)));
        return Optional.ofNullable(next).map(ZonedDateTime::toInstant);
    }

    public static boolean isValidCron(String cron) {
        return cron != null && CronExpression.isValidExpression(cron);
    }

    public static boolean isValidTimezone(String timezone) {
        try {
            ZoneId.of(timezone);
            return true;
        } catch (DateTimeException | NullPointerException e) {
            return false;
        }
    }
}
