package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Entry;

public interface SSTEntryIterator extends AutoCloseable {

    boolean hasNext();

    Entry next();

    @Override
    default void close() {}
}
