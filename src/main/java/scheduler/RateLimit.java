package scheduler;

/**
 * Rate-limit settings for a tenant: how many jobs may start in a burst,
 * and how fast tokens refill each second.
 */
public final class RateLimit {
    /** Max tokens in the bucket (burst size). */
    public final int burst;
    /** Tokens added per second. */
    public final double perSecond;

    /**
     * @param burst      must be at least 1
     * @param perSecond  must be greater than 0
     */
    public RateLimit(int burst, double perSecond) {
        if (burst < 1 || perSecond <= 0) {
            throw new IllegalArgumentException("bad rate limit");
        }
        this.burst = burst;
        this.perSecond = perSecond;
    }
}
