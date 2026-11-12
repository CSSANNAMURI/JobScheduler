package scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs jobs from many tenants on a fixed pool of worker threads.
 *
 * <p>Each tenant has its own waiting queue and rate limit. The waiting queue
 * across all tenants is bounded. Finished jobs keep a bounded history so memory
 * stays bounded. One lock protects shared state. Job code always runs outside that lock.
 */
public final class JobScheduler {

    /** Thrown when submit is refused. Submit never waits — it fails right away. */
    public static final class SubmitRejectedException extends RejectedExecutionException {
        private static final long serialVersionUID = 1L;

        /** Why submit failed: shutting down, or the global waiting queue is full. */
        public enum Reason { SHUTTING_DOWN, GLOBAL_QUEUE_FULL }

        private final Reason reason;

        /** Create a rejection with a typed reason and a human-readable message. */
        SubmitRejectedException(Reason reason, String message) {
            super(message);
            this.reason = reason;
        }

        /** Why submit was refused. */
        public Reason reason() { return reason; }
    }

    /**
     * Default finished-history size. Kept separate from {@code maxWaitingJobs} so a small
     * waiting bound does not silently forget recent job statuses (e.g. Example 1 with 25 jobs).
     */
    public static final int DEFAULT_MAX_FINISHED_JOBS_KEPT = 10_000;

    /** Hard cap: how many jobs may wait at once (across all tenants). */
    private final int maxWaitingJobs;
    /** Hard cap: how many finished jobs we keep for status() before evicting oldest. */
    private final int maxFinishedJobsKept;
    /** Rate limit used when a tenant has no override. */
    private final RateLimit defaultLimit;
    /** Optional per-tenant rate limits. */
    private final Map<String, RateLimit> overrides;
    /** Time source (FakeClock in tests, SystemClock in production). */
    private final Clock clock;
    /** Builds a RateLimiter when a tenant is seen for the first time. */
    private final RateLimiterFactory rateLimiterFactory;

    /** Guards all shared state below. */
    private final ReentrantLock lock = new ReentrantLock();
    /** Workers wait here until there is work, a shutdown, or a token refill time. */
    private final Condition workAvailable = lock.newCondition();

    /** Active and recent finished jobs by id. */
    private final Map<String, JobRecord> registry = new HashMap<>();
    /** Finished job ids in completion order (oldest first); used to bound memory. */
    private final Deque<String> finishedOrder = new ArrayDeque<>();
    /** Waiting jobs per tenant (FIFO). */
    private final Map<String, Deque<JobRecord>> tenantQueues = new HashMap<>();
    /** Rate limiter per tenant. */
    private final Map<String, RateLimiter> buckets = new HashMap<>();
    /** Tenant ids in round-robin order for fair dispatch. */
    private final List<String> tenantOrder = new ArrayList<>();
    /** Jobs already claimed (RUNNING) waiting for a worker thread to pick them up. */
    private final Deque<JobRecord> handoff = new ArrayDeque<>();
    /** Tenants whose queue became empty; checked later so idle tenant state can be dropped. */
    private final Set<String> maybeIdle = new HashSet<>();

    /** Next tenant index for round-robin. */
    private int rrNext = 0;
    /** How many jobs are waiting right now (not running). Must stay ≤ maxWaitingJobs. */
    private int waitingJobCount = 0;
    /** How many worker slots are free to claim a new job. */
    private int freeSlots;
    /** Soonest time (ms from now) a waiting tenant may get a token; used for timed waits. */
    private long nextWakeMillis = Long.MAX_VALUE;
    /** True after shutdown(); new submits are refused. */
    private boolean shutdown = false;

    /**
     * Builds a scheduler with the default token-bucket rate limiter and
     * {@link #DEFAULT_MAX_FINISHED_JOBS_KEPT} finished-history size.
     *
     * @param workerCount    how many worker threads to start
     * @param maxWaitingJobs max number of waiting jobs across all tenants
     * @param defaultLimit   rate limit for tenants with no override
     * @param overrides      optional per-tenant rate limits (may be null)
     * @param clock          time source (use FakeClock in tests)
     */
    public JobScheduler(int workerCount, int maxWaitingJobs, RateLimit defaultLimit,
                        Map<String, RateLimit> overrides, Clock clock) {
        this(workerCount, maxWaitingJobs, DEFAULT_MAX_FINISHED_JOBS_KEPT, defaultLimit, overrides, clock,
                new TokenBucketRateLimiterFactory());
    }

    /**
     * Same as the main constructor, but you can plug in a different rate-limiter factory.
     * Finished history uses {@link #DEFAULT_MAX_FINISHED_JOBS_KEPT}.
     */
    public JobScheduler(int workerCount, int maxWaitingJobs, RateLimit defaultLimit,
                        Map<String, RateLimit> overrides, Clock clock,
                        RateLimiterFactory rateLimiterFactory) {
        this(workerCount, maxWaitingJobs, DEFAULT_MAX_FINISHED_JOBS_KEPT, defaultLimit, overrides, clock,
                rateLimiterFactory);
    }

    /**
     * Full constructor: waiting bound and finished-history bound are independent.
     *
     * @param maxFinishedJobsKept how many terminal jobs {@link #status} remembers (at least 1)
     */
    public JobScheduler(int workerCount, int maxWaitingJobs, int maxFinishedJobsKept,
                        RateLimit defaultLimit, Map<String, RateLimit> overrides, Clock clock,
                        RateLimiterFactory rateLimiterFactory) {
        if (workerCount < 1 || maxWaitingJobs < 0 || maxFinishedJobsKept < 1) {
            throw new IllegalArgumentException(
                    "workers >= 1, maxWaitingJobs >= 0, maxFinishedJobsKept >= 1");
        }
        this.maxWaitingJobs = maxWaitingJobs;
        this.maxFinishedJobsKept = maxFinishedJobsKept;
        this.defaultLimit = Objects.requireNonNull(defaultLimit);
        this.overrides = overrides == null ? Map.of() : Map.copyOf(overrides);
        this.clock = Objects.requireNonNull(clock);
        this.rateLimiterFactory = Objects.requireNonNull(rateLimiterFactory);
        this.freeSlots = workerCount;

        // Any clock that advances on its own (e.g. a manual test clock) re-checks tokens right away.
        clock.addAdvanceListener(this::wake);
        for (int i = 0; i < workerCount; i++) {
            Thread t = new Thread(this::workerLoop, "worker-" + i);
            t.setDaemon(true);
            t.start();
        }
    }

    // ---------- Public API (brief shapes) ----------

    /**
     * Accept a new job for this tenant and return its id.
     * If the waiting queue is full, throws right away (does not block).
     */
    public JobId submit(TenantId tenant, Job job) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(job, "job");
        return new JobId(admit(tenant.value(), job));
    }

    /**
     * Try to cancel a waiting job.
     * Returns true only if this call cancelled it; false if not found, already running, or done.
     */
    public boolean cancel(JobId jobId) {
        Objects.requireNonNull(jobId, "jobId");
        return cancelResult(jobId.value()) == CancelResult.CANCELLED;
    }

    /** Current state of a job, or empty if the id is unknown or was evicted. */
    public Optional<JobStatus> status(JobId jobId) {
        Objects.requireNonNull(jobId, "jobId");
        return statusById(jobId.value());
    }

    // ---------- String / Runnable helpers (same behaviour; handy in tests) ----------

    /** Helper for tests: submit with plain strings / {@link Runnable}. */
    String submit(String tenantId, Runnable task) {
        requireTenant(tenantId);
        if (task == null) throw new IllegalArgumentException("task required");
        return admit(tenantId, task::run);
    }

    /** Helper for tests: boolean cancel by string id. */
    boolean cancel(String jobId) {
        return cancelResult(jobId) == CancelResult.CANCELLED;
    }

    /**
     * Cancel with a clearer result: CANCELLED, ALREADY_CANCELLED, TOO_LATE, or NOT_FOUND.
     * Same race rules as {@link #cancel(JobId)}.
     */
    CancelResult cancelResult(String jobId) {
        lock.lock();
        try {
            JobRecord job = jobId == null ? null : registry.get(jobId);
            if (job == null) return CancelResult.NOT_FOUND;
            if (job.state == JobStatus.QUEUED) {
                Deque<JobRecord> q = tenantQueues.get(job.tenantId);
                q.remove(job);
                if (q.isEmpty()) maybeIdle.add(job.tenantId);
                waitingJobCount--;
                job.state = JobStatus.CANCELLED;
                recordFinishedLocked(job);
                dispatchLocked();
                workAvailable.signalAll();
                return CancelResult.CANCELLED;
            }
            return job.state == JobStatus.CANCELLED ? CancelResult.ALREADY_CANCELLED : CancelResult.TOO_LATE;
        } finally {
            lock.unlock();
        }
    }

    /** Helper for tests: status by string id. */
    Optional<JobStatus> status(String jobId) {
        return statusById(jobId);
    }

    /** How many tenants the scheduler currently holds state for (package-private, for tests). */
    int trackedTenantCount() {
        lock.lock();
        try {
            return tenantOrder.size();
        } finally {
            lock.unlock();
        }
    }

    /** Error from a failed job, or empty if none / unknown id. Helper for tests. */
    Optional<Throwable> error(String jobId) {
        lock.lock();
        try {
            JobRecord job = jobId == null ? null : registry.get(jobId);
            return job == null ? Optional.empty() : Optional.ofNullable(job.error);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stop accepting new jobs. Waiting jobs are cancelled.
     * Jobs already running are allowed to finish.
     */
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            for (Deque<JobRecord> q : tenantQueues.values()) {
                while (!q.isEmpty()) {
                    JobRecord job = q.pollFirst();
                    waitingJobCount--;
                    job.state = JobStatus.CANCELLED;
                    recordFinishedLocked(job);
                }
            }
            // No submits are accepted after shutdown, so no tenant state is needed any more.
            tenantQueues.clear();
            buckets.clear();
            tenantOrder.clear();
            maybeIdle.clear();
            rrNext = 0;
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ask the scheduler to look at rate limits again.
     * FakeClock calls this when test time moves forward.
     */
    public void wake() {
        lock.lock();
        try {
            dispatchLocked();
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // ---------- Internals ----------

    /** Shared admit path for typed and string submit. */
    private String admit(String tenantId, Job task) {
        lock.lock();
        try {
            if (shutdown) {
                throw new SubmitRejectedException(SubmitRejectedException.Reason.SHUTTING_DOWN, "scheduler is shut down");
            }

            boolean fresh = !tenantQueues.containsKey(tenantId);
            JobRecord job = new JobRecord(UUID.randomUUID().toString(), tenantId, task);
            ensureTenantLocked(tenantId);
            enqueueLocked(job);
            dispatchLocked();

            if (job.state == JobStatus.QUEUED && waitingJobCount > maxWaitingJobs) {
                Deque<JobRecord> q = tenantQueues.get(tenantId);
                q.remove(job);
                waitingJobCount--;
                if (fresh) {
                    removeTenantLocked(tenantId);
                } else if (q.isEmpty()) {
                    // Existing tenant may now be idle; mark it so prune can reclaim after refill.
                    maybeIdle.add(tenantId);
                }
                throw new SubmitRejectedException(
                        SubmitRejectedException.Reason.GLOBAL_QUEUE_FULL, "queue full");
            }

            registry.put(job.id, job);
            workAvailable.signalAll();
            return job.id;
        } finally {
            lock.unlock();
        }
    }

    private Optional<JobStatus> statusById(String jobId) {
        lock.lock();
        try {
            JobRecord job = jobId == null ? null : registry.get(jobId);
            return job == null ? Optional.empty() : Optional.of(job.state);
        } finally {
            lock.unlock();
        }
    }

    /** Reject blank or null tenant ids. */
    private static String requireTenant(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId required");
        }
        return tenantId;
    }

    /** Create this tenant's queue and rate limiter on first use. */
    private void ensureTenantLocked(String tenantId) {
        if (!tenantQueues.containsKey(tenantId)) {
            tenantQueues.put(tenantId, new ArrayDeque<>());
            tenantOrder.add(tenantId);
            RateLimit limit = overrides.getOrDefault(tenantId, defaultLimit);
            buckets.put(tenantId, rateLimiterFactory.create(limit, clock.nowMillis()));
        }
    }

    /** Drop a tenant that was created only for a submit that then got rejected. */
    private void removeTenantLocked(String tenantId) {
        int idx = tenantOrder.indexOf(tenantId);
        if (idx < 0) return;
        tenantOrder.remove(idx);
        tenantQueues.remove(tenantId);
        buckets.remove(tenantId);
        if (idx < rrNext) rrNext--;
        rrNext = tenantOrder.isEmpty() ? 0 : rrNext % tenantOrder.size();
    }

    /** Put the job at the end of its tenant's waiting queue and bump the global count. */
    private void enqueueLocked(JobRecord job) {
        job.state = JobStatus.QUEUED;
        tenantQueues.get(job.tenantId).addLast(job);
        waitingJobCount++;
    }

    /**
     * Fill free worker slots by claiming waiting jobs that are allowed to start.
     * A claim moves QUEUED → RUNNING, spends one rate-limit token, and reserves one slot.
     */
    private void dispatchLocked() {
        long now = clock.nowMillis();
        nextWakeMillis = Long.MAX_VALUE;
        while (freeSlots > 0 && !shutdown) {
            JobRecord job = pickEligibleLocked(now);
            if (job == null) break;
            job.state = JobStatus.RUNNING;
            freeSlots--;
            handoff.addLast(job);
        }
        pruneIdleTenantsLocked(now);
        if (!handoff.isEmpty()) {
            workAvailable.signalAll();
        }
    }

    /**
     * Forget tenants that have no waiting jobs and whose rate limiter is full.
     * A full limiter behaves exactly like a new one, so this never grants extra burst.
     * Tenants that are still draining tokens stay until they refill.
     */
    private void pruneIdleTenantsLocked(long now) {
        Iterator<String> it = maybeIdle.iterator();
        while (it.hasNext()) {
            String tenant = it.next();
            Deque<JobRecord> q = tenantQueues.get(tenant);
            if (q == null || !q.isEmpty()) {
                it.remove();            // already removed, or has new work again
                continue;
            }
            if (buckets.get(tenant).isFull(now)) {
                it.remove();
                removeTenantLocked(tenant);
            }
        }
    }

    /**
     * Pick the next job that can start: walk tenants in round-robin order,
     * skip empty queues, and take a job only if the tenant has a rate-limit token.
     * Returns null if nobody can start right now.
     */
    private JobRecord pickEligibleLocked(long now) {
        int n = tenantOrder.size();
        for (int i = 0; i < n; i++) {
            int idx = (rrNext + i) % n;
            String tenant = tenantOrder.get(idx);
            Deque<JobRecord> q = tenantQueues.get(tenant);
            if (q.isEmpty()) continue;
            RateLimiter bucket = buckets.get(tenant);
            if (bucket.tryAcquire(now)) {
                JobRecord job = q.pollFirst();
                waitingJobCount--;
                if (q.isEmpty()) maybeIdle.add(tenant);
                rrNext = (idx + 1) % n;
                return job;
            }
            nextWakeMillis = Math.min(nextWakeMillis, bucket.millisUntilNextToken(now));
        }
        return null;
    }

    /**
     * Worker thread body: wait for a claimed job, run it without the lock,
     * then mark SUCCEEDED or FAILED and free the slot.
     */
    private void workerLoop() {
        while (true) {
            JobRecord job;
            lock.lock();
            try {
                while (handoff.isEmpty()) {
                    if (shutdown) return;
                    dispatchLocked();
                    if (!handoff.isEmpty()) break;
                    if (nextWakeMillis == Long.MAX_VALUE) {
                        workAvailable.await();
                    } else {
                        workAvailable.awaitNanos(TimeUnit.MILLISECONDS.toNanos(Math.max(1, nextWakeMillis)));
                    }
                }
                job = handoff.pollFirst();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }

            Throwable failure = null;
            try {
                job.task.run();
            } catch (Throwable t) {
                failure = t;
            }

            lock.lock();
            try {
                job.error = failure;
                job.state = failure == null ? JobStatus.SUCCEEDED : JobStatus.FAILED;
                recordFinishedLocked(job);
                freeSlots++;
                dispatchLocked();
                workAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Remember a finished job and drop the oldest finished entries once we
     * exceed {@link #maxFinishedJobsKept}, so the registry stays memory-bounded.
     */
    private void recordFinishedLocked(JobRecord job) {
        finishedOrder.addLast(job.id);
        while (finishedOrder.size() > maxFinishedJobsKept) {
            String oldId = finishedOrder.pollFirst();
            JobRecord old = registry.get(oldId);
            if (old != null && old.state.isTerminal()) {
                registry.remove(oldId);
            }
        }
    }
}
