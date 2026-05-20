package org.qwh.pms.core.config;

public record SinkConfig(
    int intervalMs,
    int maxPendingSsts
) {
    public static final int DEFAULT_INTERVAL_MS = 30000;
    public static final int DEFAULT_MAX_PENDING_SSTS = 8;

    public SinkConfig {
        if (intervalMs <= 0) intervalMs = DEFAULT_INTERVAL_MS;
        if (maxPendingSsts <= 0) maxPendingSsts = DEFAULT_MAX_PENDING_SSTS;
    }
}
