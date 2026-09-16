package org.qwh.pms.lookup.parquet;

import org.apache.paimon.options.Options;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Per-file cache for Parquet footer and lazily loaded page indexes. */
public final class ParquetLookupMetadataCache {

    public static final int DEFAULT_MAX_ENTRIES = 1_024;

    private final Options options;
    private final int maxEntries;
    private final Map<ResolvedDataFileKey, ParquetLookupMetadata> cache =
            new LinkedHashMap<>(16, 0.75F, true);
    // Guarded by cache. Invalidations must also fence loads performed outside the lock.
    private long generation;

    public ParquetLookupMetadataCache(Options options) {
        this(options, DEFAULT_MAX_ENTRIES);
    }

    public ParquetLookupMetadataCache(Options options, int maxEntries) {
        this.options = options;
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
    }

    public ParquetLookupMetadata getOrLoad(ResolvedDataFileKey key, ResolvedDataFile file)
            throws IOException {
        final long loadGeneration;
        synchronized (cache) {
            ParquetLookupMetadata cached = cache.get(key);
            if (cached != null) {
                return cached;
            }
            loadGeneration = generation;
        }

        ParquetLookupMetadata loaded = load(file);
        synchronized (cache) {
            // The in-flight caller may finish, but cannot resurrect an invalidated entry.
            if (generation != loadGeneration) {
                return loaded;
            }
            ParquetLookupMetadata existing = cache.get(key);
            if (existing != null) {
                return existing;
            }
            cache.put(key, loaded);
            evictIfNeeded();
            return loaded;
        }
    }

    public void invalidate(String fileName) {
        synchronized (cache) {
            generation++;
            cache.keySet().removeIf(key -> key.fileName().equals(fileName));
        }
    }

    public void clear() {
        synchronized (cache) {
            generation++;
            cache.clear();
        }
    }

    public int size() {
        synchronized (cache) {
            return cache.size();
        }
    }

    private void evictIfNeeded() {
        while (cache.size() > maxEntries) {
            Iterator<ResolvedDataFileKey> iterator = cache.keySet().iterator();
            iterator.next();
            iterator.remove();
        }
    }

    private ParquetLookupMetadata load(ResolvedDataFile file) throws IOException {
        return ParquetLookupMetadata.load(file, options);
    }
}
