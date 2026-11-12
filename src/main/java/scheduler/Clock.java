package scheduler;

/** Source of time. Injected so tests can control it with FakeClock. */
public interface Clock {

    /** Current time in milliseconds. */
    long nowMillis();

    /**
     * Register a listener to run whenever this clock jumps forward on its own
     * (for example, a manual test clock being advanced). The scheduler uses this
     * to re-check rate-limit tokens right away instead of waiting for real time.
     * Clocks that only move in real time can ignore it, which is the default.
     */
    default void addAdvanceListener(Runnable listener) {
    }
}
