package scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static scheduler.TestSupport.blockOn;
import static scheduler.TestSupport.check;
import static scheduler.TestSupport.make;
import static scheduler.TestSupport.st;
import static scheduler.TestSupport.test;
import static scheduler.TestSupport.waitUntil;

/** Cancel/pickup races and multi-threaded stress tests. */
final class SchedulerConcurrencyTests {

    /** Not meant to be created — call {@link #run()} only. */
    private SchedulerConcurrencyTests() {}

    /** Run the C5 cancel race and concurrent stress tests. */
    static void run() {
        // ---- Cancel vs worker pickup race ----
        test("race: cancel vs worker pickup (3000 rounds, exactly one winner)", () -> {
            var s = make(1, 5, 1_000_000, 1_000_000.0, new FakeClock());
            ExecutorService pool = Executors.newFixedThreadPool(2);
            int cancelWins = 0, workerWins = 0;
            for (int round = 0; round < 3000; round++) {
                var blockerGate = new CountDownLatch(1);
                s.submit("Tata", blockOn(blockerGate));          // occupies the only worker
                AtomicBoolean ran = new AtomicBoolean(false);
                String victim = s.submit("Tata", () -> ran.set(true));   // waits in queue
                var barrier = new CyclicBarrier(2);
                Future<?> releaser = pool.submit(() -> {
                    try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                    blockerGate.countDown();                      // worker becomes free -> picks victim
                });
                Future<CancelResult> canceller = pool.submit(() -> {
                    barrier.await();
                    return s.cancelResult(victim);
                });
                try { releaser.get(); } catch (Exception e) { throw new RuntimeException(e); }
                CancelResult r;
                try { r = canceller.get(); } catch (Exception e) { throw new RuntimeException(e); }

                if (r == CancelResult.CANCELLED) {
                    cancelWins++;
                    check(st(s, victim) == JobStatus.CANCELLED, "cancelled state");
                    // One worker + FIFO per tenant: if the victim had been claimed, it would
                    // finish before this sentinel. So once the sentinel completes, we know.
                    String sentinel = s.submit("Tata", () -> {});
                    waitUntil(() -> st(s, sentinel) == JobStatus.SUCCEEDED, "sentinel completes");
                    check(!ran.get(), "cancelled job must never run");
                } else {
                    workerWins++;
                    check(r == CancelResult.TOO_LATE, "loser is told TOO_LATE");
                    waitUntil(() -> st(s, victim) == JobStatus.SUCCEEDED, "victim completes");
                    check(ran.get(), "job ran");
                }
            }
            pool.shutdownNow();
            s.shutdown();
            System.out.println("      (cancel won " + cancelWins + ", worker won " + workerWins + ")");
        });

        // ---- Concurrency stress ----
        test("stress: 16 threads x 200 submits never exceed workers + queue", () -> {
            var s = make(2, 10, 1_000_000, 1_000_000.0, new FakeClock());
            var gate = new CountDownLatch(1);               // nothing finishes while we submit
            var start = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger(), rejected = new AtomicInteger();
            List<String> ids = Collections.synchronizedList(new ArrayList<>());
            String[] tenants = { "Tata", "Infosys", "Wipro", "Reliance" };

            ExecutorService pool = Executors.newFixedThreadPool(16);
            List<Future<?>> fs = new ArrayList<>();
            for (int th = 0; th < 16; th++) {
                String tenant = tenants[th % tenants.length];
                fs.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 200; i++) {
                        try { ids.add(s.submit(tenant, blockOn(gate))); accepted.incrementAndGet(); }
                        catch (RejectedExecutionException e) { rejected.incrementAndGet(); }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var f : fs) { try { f.get(); } catch (Exception e) { throw new RuntimeException(e); } }

            check(accepted.get() == 12, "accepted exactly workers + queue = 12, was " + accepted.get());
            check(rejected.get() == 3200 - 12, "rejected the rest, was " + rejected.get());
            long running = ids.stream().filter(id -> st(s, id) == JobStatus.RUNNING).count();
            long queued = ids.stream().filter(id -> st(s, id) == JobStatus.QUEUED).count();
            check(running == 2 && queued == 10, "running=" + running + " queued=" + queued);
            gate.countDown();
            pool.shutdownNow();
            s.shutdown();
        });

        test("stress: no job is lost or run twice (mixed submit, cancel, run)", () -> {
            var s = make(4, 50, 1_000_000, 1_000_000.0, new FakeClock());
            Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();   // token -> times run
            Map<String, String> idToToken = new ConcurrentHashMap<>();
            Set<String> cancelled = ConcurrentHashMap.newKeySet();
            // Count open work without relying on status() forever (finished history is bounded).
            AtomicInteger outstanding = new AtomicInteger();
            var start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(8);
            List<Future<?>> fs = new ArrayList<>();
            for (int th = 0; th < 8; th++) {
                int threadNo = th;
                String tenant = (th % 2 == 0) ? "Tata" : "Infosys";
                fs.add(pool.submit(() -> {
                    start.await();
                    Random rnd = new Random();
                    for (int i = 0; i < 500; i++) {
                        String token = threadNo + "-" + i;      // known BEFORE submit
                        try {
                            outstanding.incrementAndGet();
                            String id = s.submit(tenant, () -> {
                                try {
                                    runs.computeIfAbsent(token, k -> new AtomicInteger()).incrementAndGet();
                                } finally {
                                    outstanding.decrementAndGet();
                                }
                            });
                            idToToken.put(id, token);
                            if (rnd.nextInt(4) == 0 && s.cancel(id)) {
                                cancelled.add(id);
                                outstanding.decrementAndGet();
                            }
                        } catch (RejectedExecutionException e) {
                            outstanding.decrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var f : fs) { try { f.get(); } catch (Exception e) { throw new RuntimeException(e); } }
            waitUntil(() -> outstanding.get() == 0, "every accepted job finishes or is cancelled");

            for (String id : idToToken.keySet()) {
                AtomicInteger times = runs.get(idToToken.get(id));
                if (cancelled.contains(id)) {
                    check(times == null, id + " was cancelled but ran");
                } else {
                    check(times != null && times.get() == 1, id + " should run exactly once");
                }
            }
            pool.shutdownNow();
            s.shutdown();
        });
    }
}
