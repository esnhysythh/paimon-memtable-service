package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;

/**
 * 当前活跃的可写 MemTable。写入线程通过 put/delete 操作数据，
 * 达到阈值后调用 freeze() 冻结为 ImmutableMemTable。
 */
public interface CurMemTable {

    void put(Key key, Value value);

    void delete(Key key);

    Value get(Key key);

    /**
     * 冻结当前 MemTable，返回一个只读的 ImmutableMemTable。
     * 调用后当前实例内部切换为空的 Map，可继续接受新写入。
     * schemaId 校验由上层 BucketDirector 处理，不在此接口传递。
     */
    ImmutableMemTable freeze();

    long estimatedSize();

    int entryCount();

    Iterator<Entry> iterator();
}
