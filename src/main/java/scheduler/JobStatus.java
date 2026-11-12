package scheduler;

/**
 * Job lifecycle. Brief names: QUEUED, RUNNING, SUCCEEDED, CANCELLED.
 * {@link #FAILED} is used when the task throws (see NOTES).
 */
public enum JobStatus {
    /** Waiting in its tenant queue. */
    QUEUED,
    /** Claimed by a worker (or running). */
    RUNNING,
    /** Finished successfully. */
    SUCCEEDED,
    /** Task threw an error (not in the brief; see NOTES). */
    FAILED,
    /** Cancelled while waiting, or cancelled on shutdown. */
    CANCELLED;

    /** True if the job will not run again. */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
