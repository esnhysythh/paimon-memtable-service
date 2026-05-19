package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;

/**
 * 冻结后的只读 MemTable。不接受写入，支持查询和引用计数管理。
 * 引用计数用于控制内存释放时机：查询持有引用期间不会被退役。
 */
public interface ImmutableMemTable {

    Value get(Key key);

    Iterator<Entry> iterator();

    long estimatedSize();

    int entryCount();

    void incrementRef();

    void decrementRef();

    long refCount();
}
