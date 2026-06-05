package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.ImmutableMemTable;

import java.util.List;
import java.util.Optional;

public interface LocalStorageManager {

    SSTMeta flushToSST(ImmutableMemTable memTable);

    SSTReadSnapshot readSnapshot(List<SSTMeta> metas);

    SSTMeta compactSSTs(List<SSTMeta> metas);

    void deleteSST(SSTMeta meta);

    Optional<SSTMeta> evictOldestSinkedSST();
}
