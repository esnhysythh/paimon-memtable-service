package org.qwh.pms.server;

public record PmsSchedulerConfig(
    int flushReconcileIntervalMs,
    int maintenanceReconcileIntervalMs,
    long visibilityMaxDelayMs,
    int newSstMaxCount,
    int sinkedSstMaxCount,
    int sinkBatchMaxBytesMb,
    int compactMaxInputSizeMb
) {
    public static final int DEFAULT_FLUSH_RECONCILE_INTERVAL_MS = 1_000;
    public static final int DEFAULT_MAINTENANCE_RECONCILE_INTERVAL_MS = 30_000;
    public static final long DEFAULT_VISIBILITY_MAX_DELAY_MS = 600_000L;
    public static final int DEFAULT_NEW_SST_MAX_COUNT = 10;
    public static final int DEFAULT_SINKED_SST_MAX_COUNT = 10;
    public static final int DEFAULT_SINK_BATCH_MAX_BYTES_MB = 1_024;
    public static final int DEFAULT_COMPACT_MAX_INPUT_SIZE_MB = 1_024;

    public PmsSchedulerConfig {
        requirePositive("flush reconcile interval", flushReconcileIntervalMs);
        requirePositive("maintenance reconcile interval", maintenanceReconcileIntervalMs);
        if (visibilityMaxDelayMs <= 0) {
            throw new IllegalArgumentException("Invalid Paimon visibility max delay: " + visibilityMaxDelayMs);
        }
        requirePositive("NEW SST max count", newSstMaxCount);
        requirePositive("SINKED SST max count", sinkedSstMaxCount);
        requirePositive("Sink batch max bytes", sinkBatchMaxBytesMb);
        requirePositive("compact max input size", compactMaxInputSizeMb);
    }

    public static PmsSchedulerConfig defaults() {
        return new PmsSchedulerConfig(
            DEFAULT_FLUSH_RECONCILE_INTERVAL_MS,
            DEFAULT_MAINTENANCE_RECONCILE_INTERVAL_MS,
            DEFAULT_VISIBILITY_MAX_DELAY_MS,
            DEFAULT_NEW_SST_MAX_COUNT,
            DEFAULT_SINKED_SST_MAX_COUNT,
            DEFAULT_SINK_BATCH_MAX_BYTES_MB,
            DEFAULT_COMPACT_MAX_INPUT_SIZE_MB
        );
    }

    public long sinkBatchMaxBytes() {
        return mebibytes(sinkBatchMaxBytesMb);
    }

    public long compactMaxInputBytes() {
        return mebibytes(compactMaxInputSizeMb);
    }

    private static long mebibytes(int value) {
        return Math.multiplyExact((long) value, 1024L * 1024L);
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Invalid " + name + ": " + value);
        }
    }
}
