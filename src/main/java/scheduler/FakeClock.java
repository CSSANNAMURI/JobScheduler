package scheduler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/** Test clock: time only moves when a test calls {@link #advance(long)}. */
public final class FakeClock implements Clock {
    private final AtomicLong now = new AtomicLong();
    private final List<Runnable> advanceListeners = new CopyOnWriteArrayList<>();

    /** Current fake time in milliseconds. */
    @Override
    public long nowMillis() {
        return now.get();
    }

    /**
     * Move time forward by {@code millis} and notify every registered listener
     * (schedulers use this to re-check rate-limit tokens).
     */
    public void advance(long millis) {
        now.addAndGet(millis);
        for (Runnable r : advanceListeners) {
            r.run();
        }
    }

    /** Register a listener to run after each {@link #advance(long)}. */
    @Override
    public void addAdvanceListener(Runnable listener) {
        advanceListeners.add(Objects.requireNonNull(listener, "listener"));
    }
}
