package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.List;
import java.util.Optional;

public interface LocalStorageManager {

    SSTMeta flushToSST(ImmutableMemTable memTable);

    Optional<Value> get(SSTMeta meta, Key key);

    SSTEntryIterator openIterator(SSTMeta meta);

    SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive);

    SSTMeta compactSSTs(List<SSTMeta> metas);

    void deleteSST(SSTMeta meta);

    Optional<SSTMeta> evictOldestSinkedSST();
}
