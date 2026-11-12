package scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import static scheduler.TestSupport.blockOn;
import static scheduler.TestSupport.check;
import static scheduler.TestSupport.make;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/**
 * Core scheduler behavior: per-tenant rate limits, capacity, cancel, failure,
 * shutdown, fairness, and finished-history bounds. Real-clock regressions live in
 * {@link RealClockRegressionTests}.
 */
final class SchedulerCoreTests {

    /** Not meant to be created — call {@link #run()} only. */
    private SchedulerCoreTests() {}

    /** Run core behavior tests: limits, capacity, cancel, failure, fairness, history. */
    static void run() {
        // ---- Rate limiting ----
        test("scheduler: Tata limited, Infosys independent", () -> {
            var clock = new FakeClock();
            var s = make(5, 10, 2, 2.0, clock);
            var gate = new CountDownLatch(1);
            String t1 = s.submit("Tata", blockOn(gate));
            String t2 = s.submit("Tata", blockOn(gate));
            String t3 = s.submit("Tata", blockOn(gate));
            check(st(s, t1) == JobStatus.RUNNING, "t1 running");
            check(st(s, t2) == JobStatus.RUNNING, "t2 running");
            check(st(s, t3) == JobStatus.QUEUED, "t3 waits: Tata has no tokens");

            String i1 = s.submit("Infosys", blockOn(gate));
            check(st(s, i1) == JobStatus.RUNNING, "Infosys not affected by Tata");

            clock.advance(499);
            check(st(s, t3) == JobStatus.QUEUED, "t3 still queued at 499ms");
            clock.advance(1);
            check(st(s, t3) == JobStatus.RUNNING, "t3 runs at 500ms");
            gate.countDown();
            s.shutdown();
        });

        // ---- Queue capacity ----
        test("capacity: 2 workers, queue 3, 10 jobs -> 2 running, 3 waiting, 5 rejected", () -> {
            var s = make(2, 3, 100, 100.0, new FakeClock());
            var gate = new CountDownLatch(1);
            List<String> ids = new ArrayList<>();
            int rejected = 0;
            for (int i = 0; i < 10; i++) {
                try { ids.add(s.submit("Tata", blockOn(gate))); }
                catch (RejectedExecutionException e) { rejected++; }
            }
            long running = ids.stream().filter(id -> st(s, id) == JobStatus.RUNNING).count();
            long queued = ids.stream().filter(id -> st(s, id) == JobStatus.QUEUED).count();
            check(running == 2, "running = " + running);
            check(queued == 3, "queued = " + queued);
            check(rejected == 5, "rejected = " + rejected);

            // cancelling a queued job frees its slot immediately
            String victim = ids.stream().filter(id -> st(s, id) == JobStatus.QUEUED).findFirst().get();
            check(s.cancelResult(victim) == CancelResult.CANCELLED, "cancel queued job");
            String again = s.submit("Tata", blockOn(gate));
            check(st(s, again) == JobStatus.QUEUED, "new job accepted after cancel");
            gate.countDown();
            s.shutdown();
        });

        // ---- Cancel semantics ----
        test("cancel: running job -> TOO_LATE, unknown job -> NOT_FOUND, second cancel -> ALREADY_CANCELLED", () -> {
            var s = make(1, 3, 100, 100.0, new FakeClock());
            var gate = new CountDownLatch(1);
            String running = s.submit("Wipro", blockOn(gate));
            String queued = s.submit("Wipro", () -> { });
            check(s.cancelResult(running) == CancelResult.TOO_LATE, "running is too late");
            check(s.cancelResult(queued) == CancelResult.CANCELLED, "first cancel of a queued job");
            check(s.cancelResult(queued) == CancelResult.ALREADY_CANCELLED, "second cancel says already cancelled");
            check(s.cancelResult("job-999") == CancelResult.NOT_FOUND, "unknown");
            check(!s.cancel("job-999"), "boolean cancel is false for unknown");
            check(s.status("job-999").isEmpty(), "unknown status is empty");
            gate.countDown();
            s.shutdown();
        });

        // ---- Failure handling ----
        test("failing job -> FAILED and the worker survives", () -> {
            var s = make(1, 3, 100, 100.0, new FakeClock());
            String bad = s.submit("Infosys", () -> { throw new IllegalStateException("boom"); });
            waitUntil(() -> st(s, bad) == JobStatus.FAILED, "bad job FAILED");
            check(s.error(bad).isPresent(), "error recorded");
            String good = s.submit("Infosys", () -> {});
            waitUntil(() -> st(s, good) == JobStatus.SUCCEEDED, "next job still runs");
            s.shutdown();
        });

        // ---- Shutdown ----
        test("shutdown: queued cancelled, running finishes, new submit rejected", () -> {
            var s = make(1, 3, 100, 100.0, new FakeClock());
            var gate = new CountDownLatch(1);
            String running = s.submit("Tata", blockOn(gate));
            String queued = s.submit("Tata", () -> {});
            s.shutdown();
            check(st(s, queued) == JobStatus.CANCELLED, "queued cancelled");
            try { s.submit("Tata", () -> {}); check(false, "must reject"); }
            catch (RejectedExecutionException expected) { }
            gate.countDown();
            waitUntil(() -> st(s, running) == JobStatus.SUCCEEDED, "running finished");
        });

        // ---- Default and per-tenant limits ----
        test("default limit for unknown tenants, override for Infosys", () -> {
            var clock = new FakeClock();
            var s = new JobScheduler(10, 20, new RateLimit(1, 1.0),
                    Map.of("Infosys", new RateLimit(3, 3.0)), clock);
            var gate = new CountDownLatch(1);
            List<String> tata = new ArrayList<>(), infosys = new ArrayList<>();
            for (int i = 0; i < 3; i++) tata.add(s.submit("Tata", blockOn(gate)));
            for (int i = 0; i < 3; i++) infosys.add(s.submit("Infosys", blockOn(gate)));
            String wipro = s.submit("Wipro", blockOn(gate));     // never configured

            check(tata.stream().filter(id -> st(s, id) == JobStatus.RUNNING).count() == 1,
                    "Tata uses the default: only 1 start now");
            check(infosys.stream().allMatch(id -> st(s, id) == JobStatus.RUNNING),
                    "Infosys override allows 3 starts now");
            check(st(s, wipro) == JobStatus.RUNNING, "unknown tenant gets the default limit");

            clock.advance(1000);   // default bucket refills 1 token per second
            check(tata.stream().filter(id -> st(s, id) == JobStatus.RUNNING).count() == 2,
                    "Tata gets exactly 1 more token after 1 second");
            gate.countDown();
            s.shutdown();
        });

        test("blank or null tenant, or null task, is rejected", () -> {
            var s = make(1, 1, 5, 5.0, new FakeClock());
            for (String bad : new String[] { null, "", "   " }) {
                try { s.submit(bad, () -> {}); check(false, "must reject tenant [" + bad + "]"); }
                catch (IllegalArgumentException expected) { }
            }
            try { s.submit("Tata", null); check(false, "must reject null task"); }
            catch (IllegalArgumentException expected) { }
            s.shutdown();
        });

        // ---- Fairness ----
        test("round-robin: Infosys is not starved by 4 earlier Tata jobs", () -> {
            var clock = new FakeClock();
            var s = make(1, 20, 100, 100.0, clock);
            var order = Collections.synchronizedList(new ArrayList<String>());
            var gate = new CountDownLatch(1);
            s.submit("Tata", blockOn(gate));                      // holds the only worker
            for (int i = 1; i <= 4; i++) { int n = i; s.submit("Tata", () -> order.add("T" + n)); }
            for (int i = 1; i <= 2; i++) { int n = i; s.submit("Infosys", () -> order.add("I" + n)); }
            gate.countDown();
            waitUntil(() -> order.size() == 6, "all ran");
            check(order.indexOf("I1") < order.indexOf("T3"), "Infosys gets a turn early: " + order);
            s.shutdown();
        });

        // ---- Bounded finished history (C3) ----
        test("finished history: old completed jobs are evicted from status()", () -> {
            // Finished retention is independent of maxWaitingJobs (default is 10_000).
            // Use a tiny explicit cap so eviction is easy to see.
            var clock = new FakeClock();
            var s = new JobScheduler(2, 10, 12, new RateLimit(100, 100.0), null, clock,
                    new TokenBucketRateLimiterFactory());
            String first = s.submit("Tata", () -> {});
            waitUntil(() -> st(s, first) == JobStatus.SUCCEEDED, "first done");
            for (int i = 0; i < 12; i++) {
                String id = s.submit("Tata", () -> {});
                waitUntil(() -> st(s, id) == JobStatus.SUCCEEDED, "job done");
            }
            check(s.status(first).isEmpty(), "oldest finished job should be evicted");
            s.shutdown();
        });
    }
}
