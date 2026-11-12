package scheduler;

/**
 * Limits how fast a tenant may start jobs.
 * The scheduler only talks to this interface, so the algorithm can be swapped.
 */
public interface RateLimiter {

    /** Try to spend one start-token at this clock time. */
    boolean tryAcquire(long nowMillis);

    /** Milliseconds until the next token is ready (0 if one is ready now). */
    long millisUntilNextToken(long nowMillis);

    /**
     * True when this limiter has refilled completely, so a brand-new limiter would
     * behave identically. The scheduler drops idle tenants only when this is true,
     * so forgetting a limiter never grants extra burst. Default: never full,
     * meaning the scheduler keeps the tenant's state (safe, but not reclaimed).
     */
    default boolean isFull(long nowMillis) {
        return false;
    }
}
