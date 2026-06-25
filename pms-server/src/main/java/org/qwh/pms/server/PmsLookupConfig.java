package org.qwh.pms.server;

import java.nio.file.Path;
import java.time.Duration;

public record PmsLookupConfig(
    boolean cacheEnabled,
    Path cacheDir,
    long maxCacheBytes,
    int buildThreshold,
    int buildThreads,
    Duration buildTimeout,
    Duration retryBackoff,
    int directMetadataCacheEntries
) {
    public static final boolean DEFAULT_CACHE_ENABLED = true;
    public static final long DEFAULT_MAX_CACHE_BYTES = 3L * 1024 * 1024 * 1024;
    public static final int DEFAULT_BUILD_THRESHOLD = 3;
    public static final int DEFAULT_BUILD_THREADS = 2;
    public static final Duration DEFAULT_BUILD_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofSeconds(60);
    public static final int DEFAULT_DIRECT_METADATA_CACHE_ENTRIES = 1024;

    public PmsLookupConfig {
        if (cacheDir == null) {
            throw new IllegalArgumentException("lookup cacheDir must not be null");
        }
        cacheDir = cacheDir.toAbsolutePath().normalize();
        if (maxCacheBytes < 0) {
            throw new IllegalArgumentException("lookup maxCacheBytes must not be negative");
        }
        if (buildThreshold <= 0) {
            throw new IllegalArgumentException("lookup buildThreshold must be positive");
        }
        if (buildThreads <= 0) {
            throw new IllegalArgumentException("lookup buildThreads must be positive");
        }
        if (buildTimeout == null || buildTimeout.isZero() || buildTimeout.isNegative()) {
            throw new IllegalArgumentException("lookup buildTimeout must be positive");
        }
        if (retryBackoff == null || retryBackoff.isNegative()) {
            throw new IllegalArgumentException("lookup retryBackoff must not be negative");
        }
        if (directMetadataCacheEntries <= 0) {
            throw new IllegalArgumentException("lookup directMetadataCacheEntries must be positive");
        }
    }

    public static Path defaultCacheDir(String database, String table) {
        return Path.of(
            System.getProperty("java.io.tmpdir"),
            "pms-lookup-cache",
            safePathSegment(database) + "." + safePathSegment(table)
        );
    }

    private static String safePathSegment(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
