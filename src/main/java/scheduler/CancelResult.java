package scheduler;

/**
 * Detailed result of a cancel attempt.
 * {@link JobScheduler#cancel(JobId)} returns true only for CANCELLED; every other result is false.
 */
public enum CancelResult {
    /** This call cancelled a waiting job. */
    CANCELLED,
    /** This job was already cancelled by an earlier call. */
    ALREADY_CANCELLED,
    /** Job was already running or finished, so it is too late to cancel. */
    TOO_LATE,
    /** No job with that id. */
    NOT_FOUND
}
