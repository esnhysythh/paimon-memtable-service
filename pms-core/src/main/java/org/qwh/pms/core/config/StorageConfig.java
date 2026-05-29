package org.qwh.pms.core.config;

public record StorageConfig(
    String dir,
    long sinkedMaxSizeMb,
    int sinkedMaxCount,
    long localSstMaxRows,
    int compactThresholdMb,
    int compactMinFiles
) {
    public static final long DEFAULT_SINKED_MAX_SIZE_MB = 10240;
    public static final int DEFAULT_SINKED_MAX_COUNT = 100;
    public static final long DEFAULT_LOCAL_SST_MAX_ROWS = 0;
    public static final int DEFAULT_COMPACT_THRESHOLD_MB = 32;
    public static final int DEFAULT_COMPACT_MIN_FILES = 4;

    public StorageConfig(long sinkedMaxSizeMb, int sinkedMaxCount, int compactThresholdMb, int compactMinFiles) {
        this(null, sinkedMaxSizeMb, sinkedMaxCount, DEFAULT_LOCAL_SST_MAX_ROWS, compactThresholdMb, compactMinFiles);
    }

    public StorageConfig(String dir, long sinkedMaxSizeMb, int sinkedMaxCount, int compactThresholdMb, int compactMinFiles) {
        this(dir, sinkedMaxSizeMb, sinkedMaxCount, DEFAULT_LOCAL_SST_MAX_ROWS, compactThresholdMb, compactMinFiles);
    }

    public StorageConfig {
        if (dir != null && dir.isBlank()) {
            dir = null;
        }
        if (sinkedMaxSizeMb <= 0) sinkedMaxSizeMb = DEFAULT_SINKED_MAX_SIZE_MB;
        if (sinkedMaxCount <= 0) sinkedMaxCount = DEFAULT_SINKED_MAX_COUNT;
        if (localSstMaxRows < 0) localSstMaxRows = DEFAULT_LOCAL_SST_MAX_ROWS;
        if (compactThresholdMb <= 0) compactThresholdMb = DEFAULT_COMPACT_THRESHOLD_MB;
        if (compactMinFiles <= 0) compactMinFiles = DEFAULT_COMPACT_MIN_FILES;
    }
}
