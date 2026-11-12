package scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static scheduler.TestSupport.check;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/**
 * Literal scenarios from the take-home brief (Examples 1–4).
 * Success uses {@link JobStatus#SUCCEEDED} as in the brief.
 */
final class AssignmentExamplesTests {

    /** Not meant to be created — call {@link #run()} only. */
    private AssignmentExamplesTests() {}

    /** Run the four brief examples (rate limit, isolation, cancel, queue full). */
    static void run() {
        test("example 1: A 10/s, 25 instant jobs; advance 1s then 2s", () -> {
            var clock = new FakeClock();
            var s = new JobScheduler(4, 100, new RateLimit(10, 10.0), null, clock);
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                ids.add(s.submit("A", () -> {}));
            }
            waitUntil(() -> succeededCount(s, ids) == 10, "t=0: first 10 succeeded");
            check(ids.subList(0, 10).stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED),
                    "j1–j10 SUCCEEDED");
            check(ids.subList(10, 25).stream().allMatch(id -> st(s, id) == JobStatus.QUEUED),
                    "j11–j25 QUEUED");

            clock.advance(1000);
            waitUntil(() -> succeededCount(s, ids) == 20, "t=1s: next 10 succeeded");
            check(ids.subList(10, 20).stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED),
                    "j11–j20 SUCCEEDED");
            check(ids.subList(20, 25).stream().allMatch(id -> st(s, id) == JobStatus.QUEUED),
                    "j21–j25 still QUEUED");

            clock.advance(1000);
            waitUntil(() -> succeededCount(s, ids) == 25, "t=2s: all succeeded");
            check(ids.stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED), "all 25 SUCCEEDED");
            s.shutdown();
        });

        test("example 2: A throttled must not delay B", () -> {
            var clock = new FakeClock();
            var s = new JobScheduler(4, 100, new RateLimit(10, 10.0),
                    Map.of("A", new RateLimit(10, 10.0), "B", new RateLimit(10, 10.0)), clock);
            List<String> a = new ArrayList<>();
            List<String> b = new ArrayList<>();
            for (int i = 0; i < 25; i++) a.add(s.submit("A", () -> {}));
            for (int i = 0; i < 5; i++) b.add(s.submit("B", () -> {}));

            waitUntil(() -> succeededCount(s, a) == 10 && succeededCount(s, b) == 5, "A burst + all of B");
            check(a.subList(0, 10).stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED), "a1–a10 done");
            check(a.subList(10, 25).stream().allMatch(id -> st(s, id) == JobStatus.QUEUED), "a11–a25 queued");
            check(b.stream().allMatch(id -> st(s, id) == JobStatus.SUCCEEDED), "b1–b5 all done at t=0");
            s.shutdown();
        });

        test("example 3: cancel queued job; second cancel is false; never ran", () -> {
            var clock = new FakeClock();
            var s = new JobScheduler(4, 10, new RateLimit(1, 1.0), null, clock);
            var ran = new AtomicBoolean(false);
            String j1 = s.submit("A", () -> {});
            waitUntil(() -> st(s, j1) == JobStatus.SUCCEEDED, "j1 runs immediately (only token)");
            String j2 = s.submit("A", () -> ran.set(true));
            check(st(s, j2) == JobStatus.QUEUED, "j2 QUEUED (no tokens)");

            check(s.cancel(j2), "first cancel returns true");
            check(st(s, j2) == JobStatus.CANCELLED, "j2 CANCELLED");

            clock.advance(1000);
            check(st(s, j2) == JobStatus.CANCELLED, "still CANCELLED after refill");
            check(!ran.get(), "cancelled job never ran");
            check(!s.cancel(j2), "second cancel returns false");
            s.shutdown();
        });

        test("example 4: maxQueued=3; fifth submit fails fast", () -> {
            var clock = new FakeClock();
            var gate = new CountDownLatch(1);
            var s = new JobScheduler(4, 3, new RateLimit(1, 1.0), null, clock);
            String j1 = s.submit("A", () -> { try { gate.await(); } catch (InterruptedException ignored) {} });
            waitUntil(() -> st(s, j1) == JobStatus.RUNNING, "submit 1 runs");
            String j2 = s.submit("A", () -> {});
            String j3 = s.submit("A", () -> {});
            String j4 = s.submit("A", () -> {});
            check(st(s, j2) == JobStatus.QUEUED && st(s, j3) == JobStatus.QUEUED && st(s, j4) == JobStatus.QUEUED,
                    "submits 2–4 QUEUED");
            try {
                s.submit("A", () -> {});
                check(false, "submit 5 must fail immediately");
            } catch (RejectedExecutionException e) {
                check(e instanceof JobScheduler.SubmitRejectedException, "typed queue-full error");
                check(((JobScheduler.SubmitRejectedException) e).reason()
                                == JobScheduler.SubmitRejectedException.Reason.GLOBAL_QUEUE_FULL,
                        "GLOBAL_QUEUE_FULL");
            }
            gate.countDown();
            s.shutdown();
        });
    }

    /** Count how many of the given job ids are SUCCEEDED. */
    private static long succeededCount(JobScheduler s, List<String> ids) {
        return ids.stream().filter(id -> st(s, id) == JobStatus.SUCCEEDED).count();
    }
}
