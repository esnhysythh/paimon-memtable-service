package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.ImmutableMemTable;

import java.util.List;
import java.util.Optional;

public interface LocalStorageManager {

    SSTMeta flushToSST(ImmutableMemTable memTable);

    SSTReadSnapshot readSnapshot(List<SSTMeta> metas);

    /**
     * Atomically captures all currently visible local SSTs and enters their read epoch.
     */
    SSTReadSnapshot readVisibleSnapshot();

    SSTMeta compactSSTs(List<SSTMeta> metas);

    void deleteSST(SSTMeta meta);

    Optional<SSTMeta> evictOldestSinkedSST();
}
