package org.qwh.pms.core.config;

public record MemTableConfig(
    int maxEntries,
    int maxSizeMb
) {
    public static final int DEFAULT_MAX_ENTRIES = 1_000_000;
    public static final int DEFAULT_MAX_SIZE_MB = 256;

    public MemTableConfig {
        if (maxEntries <= 0) maxEntries = DEFAULT_MAX_ENTRIES;
        if (maxSizeMb <= 0) maxSizeMb = DEFAULT_MAX_SIZE_MB;
    }

    public long maxSizeBytes() {
        return (long) maxSizeMb * 1024 * 1024;
    }
}
