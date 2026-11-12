package scheduler;

import static scheduler.TestSupport.check;
import static scheduler.TestSupport.make;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/** Smoke tests for the public typed API (TenantId / JobId), which the other suites reach only through string helpers. */
final class TypedApiTests {

    private TypedApiTests() {}

    /** Run the typed-API smoke tests. */
    static void run() {
        test("typed API: submit, status, cancel with TenantId and JobId", () -> {
            var clock = new FakeClock();
            var s = make(2, 10, 1, 1.0, clock);
            JobId first = s.submit(new TenantId("A"), () -> { });
            waitUntil(() -> s.status(first).orElseThrow() == JobStatus.SUCCEEDED, "first ran");

            JobId second = s.submit(new TenantId("A"), () -> { });   // no token left: waits
            check(s.status(second).orElseThrow() == JobStatus.QUEUED, "second is queued");
            check(s.cancel(second), "first cancel of a queued job returns true");
            check(s.status(second).orElseThrow() == JobStatus.CANCELLED, "second is CANCELLED");
            check(!s.cancel(second), "second cancel returns false");

            clock.advance(1000);
            check(s.status(second).orElseThrow() == JobStatus.CANCELLED, "cancelled job never ran after refill");
            check(s.status(new JobId("no-such-job")).isEmpty(), "unknown JobId has no status");
            s.shutdown();
        });

        test("typed API: blank TenantId is refused when built", () -> {
            boolean refused = false;
            try {
                new TenantId("   ");
            } catch (IllegalArgumentException expected) {
                refused = true;
            }
            check(refused, "blank tenant id must be refused");
        });
    }
}
