package org.qwh.pms.core.config;

public record PMSConfig(
    // ── MemTable ──
    int memtableMaxEntries,
    int memtableMaxSizeMb,

    // ── WAL ──
    String walDir,
    int walFileSizeMb,
    boolean walUseMmap,

    // ── 本地存储 ──
    long storageSinkedMaxSizeMb,
    int storageSinkedMaxCount,
    int storageCompactThresholdMb,
    int storageCompactMinFiles,

    // ── Sink ──
    int sinkIntervalMs,
    int sinkMaxPendingSsts,

    // ── 流控 ──
    int flowcontrolOverloadedImmutableCount,
    int flowcontrolOverloadedPendingSstCount,

    // ── Paimon ──
    String paimonTablePath,
    String paimonWarehouse
) {
    public static final int DEFAULT_MEMTABLE_MAX_ENTRIES = 1_000_000;
    public static final int DEFAULT_MEMTABLE_MAX_SIZE_MB = 256;
    public static final int DEFAULT_WAL_FILE_SIZE_MB = 256;
    public static final boolean DEFAULT_WAL_USE_MMAP = false;
    public static final long DEFAULT_STORAGE_SINKED_MAX_SIZE_MB = 10240;
    public static final int DEFAULT_STORAGE_SINKED_MAX_COUNT = 100;
    public static final int DEFAULT_STORAGE_COMPACT_THRESHOLD_MB = 32;
    public static final int DEFAULT_STORAGE_COMPACT_MIN_FILES = 4;
    public static final int DEFAULT_SINK_INTERVAL_MS = 30000;
    public static final int DEFAULT_SINK_MAX_PENDING_SSTS = 8;
    public static final int DEFAULT_FLOWCONTROL_OVERLOADED_IMMUTABLE_COUNT = 4;
    public static final int DEFAULT_FLOWCONTROL_OVERLOADED_PENDING_SST_COUNT = 16;

    public PMSConfig {
        if (memtableMaxEntries <= 0) memtableMaxEntries = DEFAULT_MEMTABLE_MAX_ENTRIES;
        if (memtableMaxSizeMb <= 0) memtableMaxSizeMb = DEFAULT_MEMTABLE_MAX_SIZE_MB;
        if (walFileSizeMb <= 0) walFileSizeMb = DEFAULT_WAL_FILE_SIZE_MB;
        if (storageSinkedMaxSizeMb <= 0) storageSinkedMaxSizeMb = DEFAULT_STORAGE_SINKED_MAX_SIZE_MB;
        if (storageSinkedMaxCount <= 0) storageSinkedMaxCount = DEFAULT_STORAGE_SINKED_MAX_COUNT;
        if (storageCompactThresholdMb <= 0) storageCompactThresholdMb = DEFAULT_STORAGE_COMPACT_THRESHOLD_MB;
        if (storageCompactMinFiles <= 0) storageCompactMinFiles = DEFAULT_STORAGE_COMPACT_MIN_FILES;
        if (sinkIntervalMs <= 0) sinkIntervalMs = DEFAULT_SINK_INTERVAL_MS;
        if (sinkMaxPendingSsts <= 0) sinkMaxPendingSsts = DEFAULT_SINK_MAX_PENDING_SSTS;
        if (flowcontrolOverloadedImmutableCount <= 0) flowcontrolOverloadedImmutableCount = DEFAULT_FLOWCONTROL_OVERLOADED_IMMUTABLE_COUNT;
        if (flowcontrolOverloadedPendingSstCount <= 0) flowcontrolOverloadedPendingSstCount = DEFAULT_FLOWCONTROL_OVERLOADED_PENDING_SST_COUNT;
    }
}
