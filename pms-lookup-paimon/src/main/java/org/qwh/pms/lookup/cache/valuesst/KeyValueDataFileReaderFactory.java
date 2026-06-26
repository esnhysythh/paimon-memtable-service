package org.qwh.pms.lookup.cache.valuesst;

import org.apache.paimon.KeyValue;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.qwh.pms.lookup.cache.LocalCacheBuildContext;

import java.io.IOException;

/** Creates KeyValue readers for Paimon data files. */
public interface KeyValueDataFileReaderFactory {

    RecordReader<KeyValue> createRecordReader(DataFileMeta file, LocalCacheBuildContext context)
            throws IOException;
}
