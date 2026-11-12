package scheduler;

import java.util.concurrent.CountDownLatch;

import static scheduler.TestSupport.check;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/** RateLimiter / token-bucket unit tests (behind {@link RateLimiter}). */
final class TokenBucketTests {

    /** Not meant to be created — call {@link #run()} only. */
    private TokenBucketTests() {}

    /** Run every token-bucket / RateLimiter test in this class. */
    static void run() {
        test("token bucket: 2 tokens, refill 2/sec, 499ms vs 500ms", () -> {
            RateLimiter b = new TokenBucketRateLimiterFactory().create(new RateLimit(2, 2.0), 0);
            check(b.tryAcquire(0), "1st token");
            check(b.tryAcquire(0), "2nd token");
            check(!b.tryAcquire(0), "3rd must fail at t=0");
            check(!b.tryAcquire(499), "still empty at 499ms");
            check(b.tryAcquire(500), "token back at 500ms");
        });

        test("token bucket never exceeds capacity after long idle", () -> {
            RateLimiter b = new TokenBucketRateLimiterFactory().create(new RateLimit(2, 2.0), 0);
            check(b.tryAcquire(0) && b.tryAcquire(0), "use both");
            check(b.tryAcquire(60_000) && b.tryAcquire(60_000), "2 tokens after long idle");
            check(!b.tryAcquire(60_000), "but not 3");
        });

        test("RateLimiterFactory is swappable (C4)", () -> {
            RateLimiterFactory fixed = (limit, now) -> new RateLimiter() {
                private int left = limit.burst;
                @Override public boolean tryAcquire(long nowMillis) {
                    if (left <= 0) return false;
                    left--;
                    return true;
                }
                @Override public long millisUntilNextToken(long nowMillis) {
                    return left > 0 ? 0 : Long.MAX_VALUE;
                }
            };
            var clock = new FakeClock();
            var s = new JobScheduler(2, 10, new RateLimit(2, 2.0), null, clock, fixed);
            var gate = new CountDownLatch(1);
            String a = s.submit("Tata", () -> { try { gate.await(); } catch (InterruptedException ignored) {} });
            String b = s.submit("Tata", () -> { try { gate.await(); } catch (InterruptedException ignored) {} });
            String c = s.submit("Tata", () -> {});
            waitUntil(() -> st(s, a) == JobStatus.RUNNING
                            && st(s, b) == JobStatus.RUNNING
                            && st(s, c) == JobStatus.QUEUED,
                    "fixed limiter states");
            check(st(s, c) == JobStatus.QUEUED, "3rd waits: fixed limiter spent");
            gate.countDown();
            s.shutdown();
        });
    }
}
