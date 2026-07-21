package org.qwh.pms.core.bucket.operation;

/** Execution bounds for one Sink batch. Scheduling reasons and watermarks stay outside core. */
public record SinkSelection(
    long targetSequenceId,
    long maxInputBytes
) {
    public SinkSelection {
        if (targetSequenceId <= 0) {
            throw new IllegalArgumentException("targetSequenceId must be positive");
        }
        if (maxInputBytes <= 0) {
            throw new IllegalArgumentException("maxInputBytes must be positive");
        }
    }
}
