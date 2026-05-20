package org.qwh.pms.core.config;

public record StorageConfig(
    long sinkedMaxSizeMb,
    int sinkedMaxCount,
    int compactThresholdMb,
    int compactMinFiles
) {
    public static final long DEFAULT_SINKED_MAX_SIZE_MB = 10240;
    public static final int DEFAULT_SINKED_MAX_COUNT = 100;
    public static final int DEFAULT_COMPACT_THRESHOLD_MB = 32;
    public static final int DEFAULT_COMPACT_MIN_FILES = 4;

    public StorageConfig {
        if (sinkedMaxSizeMb <= 0) sinkedMaxSizeMb = DEFAULT_SINKED_MAX_SIZE_MB;
        if (sinkedMaxCount <= 0) sinkedMaxCount = DEFAULT_SINKED_MAX_COUNT;
        if (compactThresholdMb <= 0) compactThresholdMb = DEFAULT_COMPACT_THRESHOLD_MB;
        if (compactMinFiles <= 0) compactMinFiles = DEFAULT_COMPACT_MIN_FILES;
    }
}
