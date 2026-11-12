package scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;

import static scheduler.TestSupport.check;
import static scheduler.TestSupport.make;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/** Idle tenant state is reclaimed (C3), and a custom Clock wakes the scheduler (C2). */
final class TenantCleanupTests {

    private TenantCleanupTests() {}

    /** Run every tenant-cleanup and clock-wake test. */
    static void run() {
        test("cleanup: idle tenants are forgotten once their limiter refills", () -> {
            var clock = new FakeClock();
            var s = new JobScheduler(4, 1000, new RateLimit(1, 1.0), null, clock);
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                ids.add(s.submit("tenant-" + i, () -> { }));
            }
            waitUntil(() -> ids.stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED), "all 500 ran");
            // Each tenant spent its only token at t=0, so none is full yet: state is kept.
            check(s.trackedTenantCount() == 500, "still tracked while limiters are empty");

            clock.advance(1000);                       // every bucket refills to full
            check(s.trackedTenantCount() == 0, "all idle, full tenants reclaimed after refill");
            s.shutdown();
        });

        test("cleanup: a drained tenant is not forgotten, so no extra burst", () -> {
            var clock = new FakeClock();
            var s = make(2, 10, 1, 1.0, clock);
            String a1 = s.submit("A", () -> { });
            waitUntil(() -> st(s, a1) == JobStatus.SUCCEEDED, "a1 ran on the only token");
            check(s.trackedTenantCount() == 1, "A kept: its bucket is empty, not full");
            String a2 = s.submit("A", () -> { });
            check(st(s, a2) == JobStatus.QUEUED, "a2 must wait: forgetting A would have granted a burst");
            s.shutdown();
        });

        test("cleanup: rejected submit on an existing empty tenant is marked idle", () -> {
            // maxWaitingJobs=0: any job that stays QUEUED is rejected. After A spends its
            // token, a second submit is refused and must still mark A idle for reclaim.
            var clock = new FakeClock();
            var s = new JobScheduler(1, 0, new RateLimit(1, 1.0), null, clock);
            String a1 = s.submit("A", () -> { });
            waitUntil(() -> st(s, a1) == JobStatus.SUCCEEDED, "a1 ran");
            check(s.trackedTenantCount() == 1, "A still tracked (bucket empty)");
            try {
                s.submit("A", () -> { });
                check(false, "second submit must be rejected: waiting bound is 0");
            } catch (RejectedExecutionException expected) { }
            check(s.trackedTenantCount() == 1, "A kept until the bucket refills");
            clock.advance(1000);
            check(s.trackedTenantCount() == 0, "A reclaimed after reject left the queue empty");
            s.shutdown();
        });

        test("clock: any Clock that advances wakes the scheduler, not just FakeClock", () -> {
            var clock = new ManualClock();
            var s = new JobScheduler(1, 10, new RateLimit(1, 1.0), null, clock);
            String a1 = s.submit("A", () -> { });
            waitUntil(() -> st(s, a1) == JobStatus.SUCCEEDED, "a1 ran");
            String a2 = s.submit("A", () -> { });
            check(st(s, a2) == JobStatus.QUEUED, "a2 waits for a token");
            clock.advance(1000);
            // The listener runs wake() synchronously inside advance(), so the state is already updated here.
            check(st(s, a2) != JobStatus.QUEUED, "custom clock advance woke the scheduler");
            waitUntil(() -> st(s, a2) == JobStatus.SUCCEEDED, "a2 ran after refill");
            s.shutdown();
        });
    }

    /** A test clock that is not a FakeClock: it only moves when told, and notifies listeners. */
    private static final class ManualClock implements Clock {
        private volatile long now = 0;
        private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

        @Override
        public long nowMillis() {
            return now;
        }

        @Override
        public void addAdvanceListener(Runnable listener) {
            listeners.add(listener);
        }

        void advance(long millis) {
            now += millis;
            for (Runnable r : listeners) r.run();
        }
    }
}
