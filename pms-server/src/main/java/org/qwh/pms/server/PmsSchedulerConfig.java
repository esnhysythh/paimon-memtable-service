package org.qwh.pms.server;

public record PmsSchedulerConfig(
    boolean enabled,
    int flushIntervalMs,
    int sinkIntervalMs
) {
    public static final boolean DEFAULT_ENABLED = false;
    public static final int DEFAULT_FLUSH_INTERVAL_MS = 0;

    public PmsSchedulerConfig {
        if (flushIntervalMs < 0) {
            throw new IllegalArgumentException("Invalid scheduler flush interval: " + flushIntervalMs);
        }
        if (sinkIntervalMs < 0) {
            throw new IllegalArgumentException("Invalid scheduler sink interval: " + sinkIntervalMs);
        }
    }

    public static PmsSchedulerConfig disabled(int sinkIntervalMs) {
        return new PmsSchedulerConfig(false, DEFAULT_FLUSH_INTERVAL_MS, sinkIntervalMs);
    }
}
