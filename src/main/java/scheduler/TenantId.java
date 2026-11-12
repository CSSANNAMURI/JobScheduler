package scheduler;

import java.util.Objects;

/** Tenant that owns a job. Brief name: TenantId. */
public final class TenantId {
    private final String value;

    public TenantId(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("tenantId required");
        }
        this.value = value;
    }

    /** Underlying string value. */
    public String value() { return value; }

    @Override
    public boolean equals(Object o) {
        return o instanceof TenantId other && value.equals(other.value);
    }

    @Override
    public int hashCode() { return value.hashCode(); }

    @Override
    public String toString() { return value; }
}
