package scheduler;

/** Builds a new rate limiter for a tenant when that tenant first submits a job. */
public interface RateLimiterFactory {

    /**
     * Create a limiter from the given settings.
     *
     * @param limit     burst size and refill rate
     * @param nowMillis current clock time
     */
    RateLimiter create(RateLimit limit, long nowMillis);
}
