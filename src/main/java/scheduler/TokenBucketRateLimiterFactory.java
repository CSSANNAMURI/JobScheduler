package scheduler;

/** Default factory: each tenant gets a token-bucket limiter. */
public final class TokenBucketRateLimiterFactory implements RateLimiterFactory {

    /** Create a token bucket using the burst and per-second values from {@code limit}. */
    @Override
    public RateLimiter create(RateLimit limit, long nowMillis) {
        return new TokenBucket(limit.burst, limit.perSecond, nowMillis);
    }
}
