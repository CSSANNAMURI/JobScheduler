package scheduler;

import java.util.Objects;

/** Opaque id returned from {@link JobScheduler#submit}. Brief name: JobId. */
public final class JobId {
    private final String value;

    public JobId(String value) {
        this.value = Objects.requireNonNull(value, "job id");
    }

    /** Underlying string value (also used as map key). */
    public String value() { return value; }

    @Override
    public boolean equals(Object o) {
        return o instanceof JobId other && value.equals(other.value);
    }

    @Override
    public int hashCode() { return value.hashCode(); }

    @Override
    public String toString() { return value; }
}
