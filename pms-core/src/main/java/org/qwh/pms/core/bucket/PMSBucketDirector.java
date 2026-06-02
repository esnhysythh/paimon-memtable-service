package org.qwh.pms.core.bucket;

import java.util.Optional;
import java.util.List;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.storage.SSTMeta;

public interface PMSBucketDirector {

    void put(byte[] key, byte[] value);

    void delete(byte[] key);

    Optional<byte[]> get(byte[] key);

    Optional<Value> lookup(byte[] key);

    List<Entry> scan(byte[] startInclusive, Optional<byte[]> endExclusive);

    List<Entry> prefixScan(byte[] prefix);

    void freezeCurMemTable();

    void flushImmutableMemTable();

    void sinkToPaimon();

    Optional<SSTMeta> evictOldestSinkedSST();

    void compactLocalSSTs();

    // TODO: 待 Mem 缓存退化模块实现后补充以下方法
    // void degradeMemCache();

    BucketStateSnapshot stateSnapshot();

    void close();
}
