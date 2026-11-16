package dev.scheduler.worker;

import dev.scheduler.job.Job;
import dev.scheduler.job.JobRepository;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * One worker: a poller thread that claims due jobs and runs each on its own virtual thread,
 * plus a heartbeat thread that proves the worker is alive.
 *
 * <p>A semaphore of {@code concurrency} permits bounds running jobs. The poller only claims
 * as many jobs as it holds permits for, so it never takes work it cannot start at once
 * (claimed-but-waiting jobs would be invisible to other, idle workers). Handlers run with no
 * database transaction open; the claim and the final ack are each one short statement.
 *
 * <p>Each running job is an {@link Execution} with a single-assignment outcome. Exactly one
 * of four parties settles it: the handler finishing, the timeout watchdog, shutdown release,
 * or self-fencing. Whoever wins writes the result to the database; everyone else stands down.
 */
public class Worker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    private enum State {
        RUNNING,
        FINISHED,
        TIMED_OUT,
        RELEASED,
        ABANDONED
    }

    /** One claimed job being executed. {@code workerId} is the identity it was claimed under. */
    private static final class Execution {
        final Job job;
        final UUID workerId;
        final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
        volatile Thread thread;

        Execution(Job job, UUID workerId) {
            this.job = job;
            this.workerId = workerId;
        }

        /** Asks the handler to stop. Cooperative: a handler may ignore it. */
        void interrupt() {
            Thread t = thread;
            if (t != null) {
                t.interrupt();
            }
        }
    }

    private final JobRepository jobs;
    private final WorkerRepository workers;
    private final HandlerRegistry handlers;
    private final WorkerProperties properties;
    private final Semaphore slots;
    private final BackoffPolicy backoff;
    private final Set<Execution> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicInteger stuckHandlers = new AtomicInteger();
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("job-", 0).factory());
    private final ScheduledExecutorService timeoutTimer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("job-timeouts").daemon(true).factory());

    private volatile UUID id = UUID.randomUUID();
    private volatile boolean running;
    private Thread poller;
    private Thread heartbeat;

    public Worker(JobRepository jobs, WorkerRepository workers, HandlerRegistry handlers, WorkerProperties properties) {
        this.jobs = jobs;
        this.workers = workers;
        this.handlers = handlers;
        this.properties = properties;
        this.slots = new Semaphore(properties.concurrency());
        this.backoff = new BackoffPolicy(properties.retry(), new Random()); // java.util.Random is thread-safe
    }

    /** The current identity. Changes if the worker is declared dead and re-registers. */
    public UUID id() {
        return id;
    }

    /** Handlers that were timed out but have not yet returned; each still occupies a slot. */
    public int stuckHandlers() {
        return stuckHandlers.get();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        workers.register(id, hostname());
        running = true;
        String shortId = id.toString().substring(0, 8);
        // A dedicated platform thread: heartbeats must not queue behind job execution.
        heartbeat = Thread.ofPlatform().name("worker-heartbeat-" + shortId).daemon(true).start(this::heartbeatLoop);
        poller = Thread.ofPlatform().name("worker-poller-" + shortId).daemon(true).start(this::pollLoop);
        log.info("Worker {} started (concurrency={}, pollInterval={})",
                id, properties.concurrency(), properties.pollInterval());
    }

    /**
     * Graceful shutdown: stop claiming, give running jobs the grace period, then hand
     * anything unfinished back to the queue without charging it an attempt. The heartbeat
     * keeps going until the end so the worker is not reaped while it drains.
     */
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
                releaseUnfinished();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        timeoutTimer.shutdownNow();
        heartbeat.interrupt();
        try {
            heartbeat.join(1_000);
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

    /**
     * Test hook: stop the poller and heartbeat the way {@code kill -9} would, touching nothing
     * in the database. Handlers already running keep running, as a zombie's would.
     */
    void crash() {
        running = false;
        poller.interrupt();
        heartbeat.interrupt();
    }

    // ---- polling ----

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
                    Thread.sleep(jittered(properties.pollInterval().toMillis()));
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
        UUID claimingAs = id;
        List<Job> claimed;
        try {
            claimed = jobs.claim(claimingAs, free);
        } catch (RuntimeException e) {
            slots.release(free);
            throw e;
        }
        slots.release(free - claimed.size());
        for (Job job : claimed) {
            var execution = new Execution(job, claimingAs);
            inFlight.add(execution);
            executor.execute(() -> {
                try {
                    run(execution);
                } finally {
                    inFlight.remove(execution);
                    // Only now, when the thread is really done, does the slot come back.
                    slots.release();
                }
            });
        }
        return claimed.size() == free;
    }

    // ---- execution ----

    private void run(Execution execution) {
        Job job = execution.job;
        execution.thread = Thread.currentThread();
        if (execution.state.get() != State.RUNNING) {
            return; // released or abandoned before the handler started
        }
        ScheduledFuture<?> watchdog = timeoutTimer.schedule(
                // The timer thread only dispatches; the database write happens off it.
                () -> Thread.startVirtualThread(() -> timeOut(execution)), job.timeoutSeconds(), TimeUnit.SECONDS);

        String error = null;
        boolean retryable = true;
        try {
            var handler = handlers.find(job.type());
            if (handler.isEmpty()) {
                // No amount of retrying on this worker fleet will make a handler appear.
                error = "No handler registered for job type '" + job.type() + "'";
                retryable = false;
            } else {
                handler.get().handle(new JobContext(job.id(), job.type(), job.payload(), job.attempts()));
            }
        } catch (NonRetryableException e) {
            error = describe(e);
            retryable = false;
        } catch (Throwable t) {
            error = describe(t);
        } finally {
            watchdog.cancel(false);
        }

        if (!execution.state.compareAndSet(State.RUNNING, State.FINISHED)) {
            // Someone else already settled this attempt; its result must not be written.
            State settledAs = execution.state.get();
            if (settledAs == State.TIMED_OUT) {
                stuckHandlers.decrementAndGet();
            }
            log.info("Job {} attempt {} returned after being {}; result discarded",
                    job.id(), job.attempts(), settledAs);
            return;
        }
        try {
            // Fenced by (worker, attempt): a stale result means this attempt no longer owns the job.
            boolean recorded;
            if (error == null) {
                recorded = jobs.markSucceeded(job.id(), execution.workerId, job.attempts());
            } else {
                recorded = jobs.markFailed(
                                job.id(), execution.workerId, job.attempts(), error, retryable, nextDelay(job))
                        .isPresent();
            }
            if (!recorded) {
                log.warn("Worker {} discarded the result of job {} attempt {}: no longer the owner",
                        execution.workerId, job.id(), job.attempts());
            }
        } catch (RuntimeException e) {
            // The job stays RUNNING under this worker's name until crash recovery reclaims it.
            log.error("Worker {} could not record the outcome of job {} attempt {}",
                    execution.workerId, job.id(), job.attempts(), e);
        }
    }

    /**
     * Watchdog: the handler exceeded the job's timeout. Fail the attempt now (so the job can be
     * retried without waiting for the handler) and interrupt the handler. If the handler ignores
     * the interrupt it keeps its slot until it returns.
     */
    private void timeOut(Execution execution) {
        if (!execution.state.compareAndSet(State.RUNNING, State.TIMED_OUT)) {
            return;
        }
        stuckHandlers.incrementAndGet();
        Job job = execution.job;
        log.warn("Job {} attempt {} exceeded its {}s timeout", job.id(), job.attempts(), job.timeoutSeconds());
        try {
            jobs.markTimedOut(
                    job.id(),
                    execution.workerId,
                    job.attempts(),
                    "Timed out after " + job.timeoutSeconds() + "s",
                    nextDelay(job));
        } catch (RuntimeException e) {
            log.error("Worker {} could not record the timeout of job {} attempt {}",
                    execution.workerId, job.id(), job.attempts(), e);
        }
        execution.interrupt();
    }

    /** Shutdown ran out of grace: return still-running jobs to the queue, attempt excused. */
    private void releaseUnfinished() {
        for (Execution execution : inFlight) {
            if (!execution.state.compareAndSet(State.RUNNING, State.RELEASED)) {
                continue;
            }
            Job job = execution.job;
            try {
                jobs.release(job.id(), execution.workerId, job.attempts());
                log.info("Worker {} released job {} attempt {} at shutdown", id, job.id(), job.attempts());
            } catch (RuntimeException e) {
                log.error("Worker {} could not release job {}; the reaper will reclaim it", id, job.id(), e);
            }
            execution.interrupt();
        }
    }

    // ---- heartbeat and self-fencing ----

    private void heartbeatLoop() {
        while (running) {
            try {
                Thread.sleep(properties.heartbeatInterval().toMillis());
                if (!workers.heartbeat(id)) {
                    fenceSelf();
                }
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                // If this lasts past the heartbeat timeout we will be reaped, then fenced above.
                log.error("Worker {} failed to heartbeat; will retry", id, e);
            }
        }
    }

    /**
     * A reaper declared this worker dead (it was stalled or cut off for too long) and has
     * given its jobs to others. Abandon everything claimed under the old identity and carry
     * on as a brand-new worker.
     */
    private void fenceSelf() {
        UUID deadId = id;
        log.warn("Worker {} was declared dead; abandoning its jobs and re-registering", deadId);
        for (Execution execution : inFlight) {
            if (execution.workerId.equals(deadId) && execution.state.compareAndSet(State.RUNNING, State.ABANDONED)) {
                execution.interrupt();
            }
        }
        UUID freshId = UUID.randomUUID();
        workers.register(freshId, hostname());
        id = freshId; // only after the row exists; until then claims under deadId return nothing
        log.info("Worker {} re-registered as {}", deadId, freshId);
    }

    // ---- helpers ----

    private java.time.Duration nextDelay(Job job) {
        return backoff.delay(job.attempts() - job.attemptsExcused());
    }

    /** {@code base} +/- 10%, so workers started together do not query in lockstep. */
    private static long jittered(long base) {
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
