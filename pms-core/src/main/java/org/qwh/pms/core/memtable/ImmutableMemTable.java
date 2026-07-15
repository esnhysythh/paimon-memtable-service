package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Optional;

/**
 * 冻结后的只读 MemTable。不接受写入。Flush 发布后由 SST 接管查询，
 * ImmutableMemTable 随即退出查询路径，不承担长期 cache 职责。
 */
public interface ImmutableMemTable {

    Value get(Key key);

    Iterator<Entry> iterator();

    Iterator<Entry> iterator(Key startInclusive, Optional<Key> endExclusive);

    long estimatedSize();

    int estimatedEntryCount();

    long minSequenceId();

    long maxSequenceId();

    long oldestWriteAtMillis();
}
