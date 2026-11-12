package scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Shared helpers for plain-Java test suites (no JUnit). */
final class TestSupport {

    /** How many tests have passed so far in this run. */
    static int passed = 0;

    /** Not meant to be created — static helpers only. */
    private TestSupport() {}

    /** Fail the test with a clear message if {@code cond} is false. */
    static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError("FAILED: " + msg);
    }

    /** Run one test case, count it as passed, and print PASS. */
    static void test(String name, Runnable body) {
        body.run();
        passed++;
        System.out.println("PASS  " + name);
    }

    /**
     * Wait until {@code c} becomes true (real wall time).
     * Used when a worker thread must finish before we can assert.
     * Fails after about 5 seconds.
     */
    static void waitUntil(java.util.function.BooleanSupplier c, String msg) {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!c.getAsBoolean()) {
            if (System.nanoTime() > end) throw new AssertionError("TIMEOUT: " + msg);
            Thread.onSpinWait();
        }
    }

    /**
     * Like {@link #waitUntil} but with a caller-chosen bound in milliseconds.
     * Returns whether the condition became true in time (spin-wait only; meets C2).
     */
    static boolean waitFor(java.util.function.BooleanSupplier c, long millis) {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (!c.getAsBoolean()) {
            if (System.nanoTime() > end) return false;
            Thread.onSpinWait();
        }
        return true;
    }

    /** Read a job's state or fail if the id is unknown. */
    static JobStatus st(JobScheduler s, String id) {
        return s.status(id).orElseThrow();
    }

    /** A task that blocks until {@code latch} is released (keeps a worker busy). */
    static Runnable blockOn(CountDownLatch latch) {
        return () -> { try { latch.await(); } catch (InterruptedException ignored) {} };
    }

    /** Build a scheduler with workers, max waiting jobs, default rate limit, and FakeClock. */
    static JobScheduler make(int workers, int maxWaitingJobs, int burst, double perSec, FakeClock clock) {
        return new JobScheduler(workers, maxWaitingJobs, new RateLimit(burst, perSec), null, clock);
    }
}
