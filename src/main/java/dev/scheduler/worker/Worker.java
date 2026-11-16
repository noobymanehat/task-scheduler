package dev.scheduler.worker;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobRepository;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * One worker: a poller thread that claims due jobs and runs each on its own virtual thread.
 *
 * <p>A semaphore of {@code concurrency} permits bounds running jobs. The poller only claims
 * as many jobs as it holds permits for, so it never takes work it cannot start at once
 * (claimed-but-waiting jobs would be invisible to other, idle workers). Handlers run with no
 * database transaction open; the claim and the final ack are each one short statement.
 */
public class Worker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    private final UUID id = UUID.randomUUID();
    private final JobRepository jobs;
    private final WorkerRepository workers;
    private final HandlerRegistry handlers;
    private final WorkerProperties properties;
    private final Semaphore slots;
    private final BackoffPolicy backoff;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("job-", 0).factory());

    private volatile boolean running;
    private Thread poller;

    public Worker(JobRepository jobs, WorkerRepository workers, HandlerRegistry handlers, WorkerProperties properties) {
        this.jobs = jobs;
        this.workers = workers;
        this.handlers = handlers;
        this.properties = properties;
        this.slots = new Semaphore(properties.concurrency());
        this.backoff = new BackoffPolicy(properties.retry(), new Random()); // java.util.Random is thread-safe
    }

    public UUID id() {
        return id;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        workers.register(id, hostname());
        running = true;
        poller = Thread.ofPlatform()
                .name("worker-poller-" + id.toString().substring(0, 8))
                .daemon(true)
                .start(this::pollLoop);
        log.info("Worker {} started (concurrency={}, pollInterval={})",
                id, properties.concurrency(), properties.pollInterval());
    }

    /** Stops claiming, then waits up to the grace period for running jobs to finish. */
    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        poller.interrupt();
        long graceMillis = properties.shutdownGrace().toMillis();
        try {
            // Join the poller first so no claimed batch is dispatched after the executor closes.
            poller.join(graceMillis);
            executor.shutdown();
            if (!executor.awaitTermination(graceMillis, TimeUnit.MILLISECONDS)) {
                log.warn("Worker {} stopped with jobs still running after {}", id, properties.shutdownGrace());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            workers.markStopped(id);
        } catch (RuntimeException e) {
            log.warn("Worker {} could not record its shutdown", id, e);
        }
        log.info("Worker {} stopped", id);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void pollLoop() {
        while (running) {
            boolean fullBatch = false;
            try {
                fullBatch = pollOnce();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("Worker {} failed to claim jobs; will retry", id, e);
            }
            // A full batch means there is probably more work: poll again without waiting.
            if (!fullBatch) {
                try {
                    Thread.sleep(jitteredPollInterval());
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    /** Claims up to as many jobs as there are free slots. Returns true if the batch was full. */
    private boolean pollOnce() throws InterruptedException {
        slots.acquire(); // blocks while every slot is busy
        int free = 1 + slots.drainPermits();
        List<Job> claimed;
        try {
            claimed = jobs.claim(id, free);
        } catch (RuntimeException e) {
            slots.release(free);
            throw e;
        }
        slots.release(free - claimed.size());
        for (Job job : claimed) {
            executor.execute(() -> {
                try {
                    execute(job);
                } finally {
                    slots.release();
                }
            });
        }
        return claimed.size() == free;
    }

    private void execute(Job job) {
        String error = null;
        boolean retryable = true;
        var handler = handlers.find(job.type());
        if (handler.isEmpty()) {
            // No amount of retrying on this worker fleet will make a handler appear.
            error = "No handler registered for job type '" + job.type() + "'";
            retryable = false;
        } else {
            try {
                handler.get().handle(new JobContext(job.id(), job.type(), job.payload(), job.attempts()));
            } catch (NonRetryableException e) {
                error = describe(e);
                retryable = false;
            } catch (Throwable t) {
                error = describe(t);
            }
        }
        try {
            // Fenced by (worker, attempt): a stale result means this attempt no longer owns the job.
            boolean recorded;
            if (error == null) {
                recorded = jobs.markSucceeded(job.id(), id, job.attempts());
            } else {
                int countedAttempt = job.attempts() - job.attemptsExcused();
                recorded = jobs.markFailed(
                                job.id(), id, job.attempts(), error, retryable, backoff.delay(countedAttempt))
                        .isPresent();
            }
            if (!recorded) {
                log.warn("Worker {} discarded the result of job {} attempt {}: no longer the owner",
                        id, job.id(), job.attempts());
            }
        } catch (RuntimeException e) {
            // The job stays RUNNING under this worker's name until crash recovery reclaims it.
            log.error("Worker {} could not record the outcome of job {} attempt {}",
                    id, job.id(), job.attempts(), e);
        }
    }

    /** Poll interval +/- 10%, so workers started together do not query in lockstep. */
    private long jitteredPollInterval() {
        long base = properties.pollInterval().toMillis();
        long spread = base / 10;
        return Math.max(1, base + (spread == 0 ? 0 : ThreadLocalRandom.current().nextLong(-spread, spread + 1)));
    }

    private static String describe(Throwable t) {
        String text = t.toString();
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
