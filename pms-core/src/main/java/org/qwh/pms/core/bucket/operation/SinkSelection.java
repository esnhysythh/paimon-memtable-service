package org.qwh.pms.core.bucket.operation;

/** Execution bounds for one Sink batch. Scheduling reasons and watermarks stay outside core. */
public record SinkSelection(
    long targetSequenceId,
    int maxSstCount,
    long maxInputBytes
) {
    public SinkSelection {
        if (targetSequenceId <= 0) {
            throw new IllegalArgumentException("targetSequenceId must be positive");
        }
        if (maxSstCount <= 0) {
            throw new IllegalArgumentException("maxSstCount must be positive");
        }
        if (maxInputBytes <= 0) {
            throw new IllegalArgumentException("maxInputBytes must be positive");
        }
    }

    /** Transitional selection preserving the current behavior of sinking every stable NEW run. */
    public static SinkSelection allAvailable() {
        return new SinkSelection(Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
    }
}
