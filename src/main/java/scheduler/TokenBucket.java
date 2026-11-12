package scheduler;

/**
 * Simple token-bucket rate limiter.
 * Tokens refill from the clock when asked — no background timer thread.
 * The scheduler's lock protects this object; it has no lock of its own.
 */
final class TokenBucket implements RateLimiter {
    private final double capacity;
    private final double refillPerMs;
    private double tokens;
    private long lastMillis;

    /**
     * Create a full bucket.
     *
     * @param capacity   max tokens (burst size)
     * @param perSecond  how many tokens are added each second
     * @param nowMillis  current clock time
     */
    TokenBucket(int capacity, double perSecond, long nowMillis) {
        this.capacity = capacity;
        this.refillPerMs = perSecond / 1000.0;
        this.tokens = capacity;          // starts full
        this.lastMillis = nowMillis;
    }

    /** Add tokens for time that has passed since the last update. Cap at capacity. */
    private void refill(long now) {
        if (now > lastMillis) {
            tokens = Math.min(capacity, tokens + (now - lastMillis) * refillPerMs);
            lastMillis = now;
        }
    }

    /** Take one token if available at this time. Returns false if the bucket is empty. */
    @Override
    public boolean tryAcquire(long now) {
        refill(now);
        if (tokens >= 1.0 - 1e-9) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }

    /** True when the bucket has refilled to capacity (same as a fresh bucket). */
    @Override
    public boolean isFull(long now) {
        refill(now);
        return tokens >= capacity - 1e-9;
    }

    /** How long to wait (ms) until the next token is ready. Zero if one is ready now. */
    @Override
    public long millisUntilNextToken(long now) {
        refill(now);
        if (tokens >= 1.0 - 1e-9) {
            return 0;
        }
        return (long) Math.ceil((1.0 - tokens) / refillPerMs);
    }
}
