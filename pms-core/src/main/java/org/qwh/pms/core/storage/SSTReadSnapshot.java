package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.List;
import java.util.Optional;

public interface SSTReadSnapshot extends AutoCloseable {

    List<SSTMeta> metas();

    Optional<Value> get(SSTMeta meta, Key key);

    SSTEntryIterator openIterator(SSTMeta meta);

    SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive);

    @Override
    void close();
}
