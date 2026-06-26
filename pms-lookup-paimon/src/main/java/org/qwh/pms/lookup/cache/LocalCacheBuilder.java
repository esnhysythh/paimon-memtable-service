package org.qwh.pms.lookup.cache;

import org.apache.paimon.io.DataFileMeta;

import java.io.IOException;

/** Builds a local cache entry for one Paimon data file. */
public interface LocalCacheBuilder {

    LocalCacheMode mode();

    LocalCacheEntry build(DataFileMeta file, LocalCacheBuildContext context) throws IOException;
}
