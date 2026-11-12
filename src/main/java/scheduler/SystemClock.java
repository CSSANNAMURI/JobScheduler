package scheduler;

/**
 * Real monotonic clock for production.
 * Uses {@link System#nanoTime()} so time only moves forward (good for rate limits).
 */
public final class SystemClock implements Clock {

    /** Current time in milliseconds from a monotonic clock. */
    @Override
    public long nowMillis() {
        return System.nanoTime() / 1_000_000L;
    }
}
