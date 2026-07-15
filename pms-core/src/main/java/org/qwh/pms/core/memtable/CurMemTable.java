package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Optional;

/**
 * 当前活跃的可写 MemTable。写入线程通过 put/delete 操作数据，
 * 达到阈值后调用 freeze() 冻结为 ImmutableMemTable。
 */
public interface CurMemTable {

    void put(Key key, Value value);

    Value get(Key key);

    /**
     * 冻结当前 MemTable，返回一个只读的 ImmutableMemTable。
     * 调用后当前实例被封存，不再接受新写入；调用方必须创建并发布新的
     * CurMemTable。冻结实例继续保留原数据，确保并发查询持有的旧引用有效。
     * schemaId 校验由上层 BucketDirector 处理，不在此接口传递。
     */
    ImmutableMemTable freeze();

    long estimatedSize();

    int estimatedEntryCount();

    long minSequenceId();

    long maxSequenceId();

    long oldestWriteAtMillis();

    /**
     * Check whether this MemTable has reached its capacity threshold
     * and should be frozen.
     */
    boolean shouldFreeze();

    Iterator<Entry> iterator();

    Iterator<Entry> iterator(Key startInclusive, Optional<Key> endExclusive);
}
