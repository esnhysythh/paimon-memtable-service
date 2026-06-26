package org.qwh.pms.lookup.parquet;

import org.apache.paimon.io.DataFileMeta;
import org.qwh.pms.lookup.api.ResolvedDataFile;

/** Stable identity for cached per-file Parquet lookup metadata. */
public record ResolvedDataFileKey(String fileName, String path, long fileSize, long schemaId) {

    public static ResolvedDataFileKey of(DataFileMeta file, ResolvedDataFile resolved) {
        return new ResolvedDataFileKey(
                file.fileName(), resolved.path().toString(), resolved.fileSize(), file.schemaId());
    }
}
