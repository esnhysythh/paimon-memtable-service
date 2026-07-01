package org.qwh.pms.protocol.api;

import java.util.Objects;

public record WriteResult(PmsStatus status, int acceptedCount) {

    public WriteResult {
        Objects.requireNonNull(status, "status must not be null");
        if (acceptedCount < 0) {
            throw new IllegalArgumentException("acceptedCount must not be negative: " + acceptedCount);
        }
        if (status != PmsStatus.OK && acceptedCount != 0) {
            throw new IllegalArgumentException("non-OK write result must use acceptedCount 0");
        }
    }

    public static WriteResult ok(int acceptedCount) {
        return new WriteResult(PmsStatus.OK, acceptedCount);
    }

    public static WriteResult failed(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("failed write result cannot use OK status");
        }
        return new WriteResult(status, 0);
    }
}
