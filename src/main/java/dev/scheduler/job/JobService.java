package dev.scheduler.job;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    static final int DEFAULT_PRIORITY = 0;
    static final int DEFAULT_MAX_RETRIES = 3;
    static final int DEFAULT_TIMEOUT_SECONDS = 300;
    static final int MAX_PAGE_SIZE = 200;

    private final JobRepository jobs;

    public JobService(JobRepository jobs) {
        this.jobs = jobs;
    }

    /**
     * Submits a job. With an idempotency key, a replay of an equivalent request returns the
     * original job ({@code created == false}); a replay with different contents is rejected
     * rather than silently answered with a job the caller did not ask for.
     */
    public Submission submit(SubmitJobRequest request) {
        NewJob newJob = toNewJob(request);
        var inserted = jobs.insert(newJob);
        if (inserted.isPresent()) {
            return new Submission(inserted.get(), true);
        }
        var existing = jobs.findByIdempotencyKey(newJob)
                .orElseThrow(() -> new IllegalStateException(
                        "Insert conflicted on idempotency key but no job holds it: " + newJob.idempotencyKey()));
        if (!existing.sameRequest()) {
            throw new IdempotencyConflictException(newJob.idempotencyKey(), existing.job().id());
        }
        return new Submission(existing.job(), false);
    }

    public Job get(UUID id) {
        return jobs.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public JobPage list(JobStatus status, String type, int limit, String cursor) {
        int pageSize = Math.clamp(limit, 1, MAX_PAGE_SIZE);
        Cursor after = cursor == null ? null : Cursor.decode(cursor);
        // Fetch one extra row to learn whether another page exists.
        List<Job> rows = jobs.list(
                status, type, after == null ? null : after.createdAt(), after == null ? null : after.id(), pageSize + 1);
        boolean hasMore = rows.size() > pageSize;
        List<Job> page = hasMore ? rows.subList(0, pageSize) : rows;
        String next = hasMore ? new Cursor(page.getLast().createdAt(), page.getLast().id()).encode() : null;
        return new JobPage(page.stream().map(JobResponse::from).toList(), next);
    }

    public Job cancel(UUID id) {
        return jobs.cancel(id).orElseThrow(() -> {
            // The guarded UPDATE matched nothing; look up why, purely to pick the error.
            Job current = get(id);
            return new JobNotCancellableException(id, current.status());
        });
    }

    private static NewJob toNewJob(SubmitJobRequest r) {
        return new NewJob(
                r.type(),
                r.payload() == null || r.payload().isNull() ? "{}" : r.payload().toString(),
                r.priority() == null ? DEFAULT_PRIORITY : r.priority(),
                // timestamptz holds microseconds; truncate so replays compare equal.
                r.runAt() == null ? null : r.runAt().truncatedTo(ChronoUnit.MICROS),
                r.maxRetries() == null ? DEFAULT_MAX_RETRIES : r.maxRetries(),
                r.timeoutSeconds() == null ? DEFAULT_TIMEOUT_SECONDS : r.timeoutSeconds(),
                r.idempotencyKey());
    }

    public record Submission(Job job, boolean created) {}

    /** Opaque keyset position: the (created_at, id) of the last row already returned. */
    record Cursor(Instant createdAt, UUID id) {

        String encode() {
            return Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString((createdAt + "|" + id).getBytes(StandardCharsets.UTF_8));
        }

        static Cursor decode(String cursor) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8)
                        .split("\\|", -1);
                if (parts.length != 2) {
                    throw new InvalidCursorException();
                }
                return new Cursor(Instant.parse(parts[0]), UUID.fromString(parts[1]));
            } catch (IllegalArgumentException | DateTimeParseException e) {
                throw new InvalidCursorException();
            }
        }
    }
}
