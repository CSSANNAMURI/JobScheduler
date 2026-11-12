package scheduler;

/**
 * Work unit passed to {@link JobScheduler#submit}. Brief name: Job.
 * Same shape as {@link Runnable}; kept as its own type for the assignment API.
 */
@FunctionalInterface
public interface Job {
    /** Run the job body. */
    void run();
}
