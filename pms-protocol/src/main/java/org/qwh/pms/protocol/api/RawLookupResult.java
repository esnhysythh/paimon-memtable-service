package org.qwh.pms.protocol.api;

import java.util.Objects;

public record RawLookupResult(PmsStatus status, LookupResultType type, byte[] row) {

    public RawLookupResult {
        Objects.requireNonNull(status, "status must not be null");
        if (status == PmsStatus.OK) {
            Objects.requireNonNull(type, "type must not be null for OK lookup result");
            if (type == LookupResultType.HIT) {
                Objects.requireNonNull(row, "row must not be null for HIT lookup result");
            } else if (row != null) {
                throw new IllegalArgumentException("row must be null for " + type + " lookup result");
            }
        } else {
            if (type != null) {
                throw new IllegalArgumentException("type must be null for non-OK lookup result");
            }
            if (row != null) {
                throw new IllegalArgumentException("row must be null for non-OK lookup result");
            }
        }
    }

    public static RawLookupResult hit(byte[] row) {
        return new RawLookupResult(PmsStatus.OK, LookupResultType.HIT, row);
    }

    public static RawLookupResult miss() {
        return new RawLookupResult(PmsStatus.OK, LookupResultType.MISS, null);
    }

    public static RawLookupResult deleted() {
        return new RawLookupResult(PmsStatus.OK, LookupResultType.DELETED, null);
    }

    public static RawLookupResult lookupUnavailable() {
        return failed(PmsStatus.LOOKUP_UNAVAILABLE);
    }

    public static RawLookupResult failed(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("failed lookup result cannot use OK status");
        }
        return new RawLookupResult(status, null, null);
    }

    public byte[] value() {
        return row;
    }
}
