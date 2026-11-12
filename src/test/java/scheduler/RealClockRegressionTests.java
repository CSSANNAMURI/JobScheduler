package scheduler;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static scheduler.TestSupport.check;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitFor;
import static scheduler.TestSupport.waitUntil;

/**
 * Real-clock wakeup regressions. These use {@link SystemClock}, so the scheduler
 * waits on real time. The tests wait on conditions with bounded spins only
 * (no timed pauses), so they meet C2 too.
 */
final class RealClockRegressionTests {

    /** Not meant to be created — call {@link #run()} only. */
    private RealClockRegressionTests() {}

    /** Run the two real-clock regression tests. */
    static void run() {
        // Every FakeClock test wakes workers via advance(), which hid this bug.
        // Here the only wakeup a waiting job gets is the scheduler's own timed wait on the real clock.
        test("real clock: job waiting for a token starts on refill (regression)", () -> {
            var s = new JobScheduler(2, 5, new RateLimit(1, 2.0), null, new SystemClock());
            try {
                String j1 = s.submit("Tata", () -> { });  // uses the only token
                waitUntil(() -> st(s, j1) == JobStatus.SUCCEEDED, "j1 ran");
                long submitted = System.nanoTime();
                String j2 = s.submit("Tata", () -> { });  // no token: should start ~500 ms later
                boolean started = waitFor(() -> st(s, j2) == JobStatus.SUCCEEDED, 2000);
                long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - submitted);
                check(started, "j2 still " + st(s, j2) + " after " + waitedMs + " ms (lost wakeup)");
            } finally {
                s.shutdown();
            }
        });

        test("real clock: a worker in a long timed wait wakes for another tenant's earlier token", () -> {
            // Tata's next token is ~1000 ms away, so a worker may wait until then. Infosys
            // refills every ~100 ms. Its second job must start well before Tata's deadline.
            // The gap (1000 vs 100) keeps this stable under CPU load; a 350 ms window on a
            // 500 ms Tata deadline was too tight.
            var s = new JobScheduler(2, 5, new RateLimit(1, 1.0),
                                     Map.of("Infosys", new RateLimit(1, 10.0)), new SystemClock());
            try {
                s.submit("Tata", () -> { });              // takes Tata's only token
                String t2 = s.submit("Tata", () -> { });  // waits ~1000 ms for a token
                String i1 = s.submit("Infosys", () -> { });  // takes Infosys' only token
                waitUntil(() -> st(s, i1) == JobStatus.SUCCEEDED && st(s, t2) == JobStatus.QUEUED,
                          "Tata waiting, Infosys spent");
                long submitted = System.nanoTime();
                String i2 = s.submit("Infosys", () -> { });   // token due in ~100 ms
                boolean started = waitFor(() -> st(s, i2) == JobStatus.SUCCEEDED, 700);
                long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - submitted);
                check(started, "Infosys job still " + st(s, i2) + " after " + waitedMs
                      + " ms: worker waited on Tata's later deadline");
            } finally {
                s.shutdown();
            }
        });
    }
}
