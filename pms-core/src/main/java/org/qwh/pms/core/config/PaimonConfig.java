package org.qwh.pms.core.config;

public record PaimonConfig(
    String tablePath,
    String warehouse,
    boolean cacheEnabled,
    String manifestCacheSmallFileMemory,
    String manifestCacheSmallFileThreshold,
    String manifestCacheMaxMemory
) {
    public static final boolean DEFAULT_CACHE_ENABLED = true;
    public static final String DEFAULT_MANIFEST_CACHE_SMALL_FILE_MEMORY = "128mb";
    public static final String DEFAULT_MANIFEST_CACHE_SMALL_FILE_THRESHOLD = "1mb";

    public PaimonConfig(String tablePath, String warehouse) {
        this(
            tablePath,
            warehouse,
            DEFAULT_CACHE_ENABLED,
            DEFAULT_MANIFEST_CACHE_SMALL_FILE_MEMORY,
            DEFAULT_MANIFEST_CACHE_SMALL_FILE_THRESHOLD,
            null
        );
    }

    public PaimonConfig {
        if (tablePath == null || tablePath.isBlank()) {
            throw new IllegalArgumentException("Paimon table path must not be empty");
        }
        if (manifestCacheSmallFileMemory == null || manifestCacheSmallFileMemory.isBlank()) {
            manifestCacheSmallFileMemory = DEFAULT_MANIFEST_CACHE_SMALL_FILE_MEMORY;
        }
        if (manifestCacheSmallFileThreshold == null || manifestCacheSmallFileThreshold.isBlank()) {
            manifestCacheSmallFileThreshold = DEFAULT_MANIFEST_CACHE_SMALL_FILE_THRESHOLD;
        }
        if (manifestCacheMaxMemory != null && manifestCacheMaxMemory.isBlank()) {
            manifestCacheMaxMemory = null;
        }
    }
}
