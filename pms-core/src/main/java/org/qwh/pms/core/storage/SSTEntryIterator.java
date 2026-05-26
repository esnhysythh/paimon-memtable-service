package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Entry;

import java.util.Iterator;

public interface SSTEntryIterator extends Iterator<Entry>, AutoCloseable {

    @Override
    default void close() {}
}
