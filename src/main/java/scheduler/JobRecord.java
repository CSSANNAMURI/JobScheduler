package scheduler;

/**
 * Internal record for one accepted job.
 * Fields other than id/tenant/task change only while holding the scheduler lock.
 */
final class JobRecord {
    /** Unique id returned from submit. */
    final String id;
    /** Tenant that owns this job. */
    final String tenantId;
    /** Work to run on a worker thread. */
    final Job task;

    /** Current lifecycle state. */
    JobStatus state = JobStatus.QUEUED;
    /** Set if the task threw. */
    Throwable error;

    /** Create a new internal job record. */
    JobRecord(String id, String tenantId, Job task) {
        this.id = id;
        this.tenantId = tenantId;
        this.task = task;
    }
}
