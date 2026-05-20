package org.qwh.pms.core.bucket;

import java.util.Optional;

public interface PMSBucketDirector {

    void put(byte[] key, byte[] value);

    void delete(byte[] key);

    Optional<byte[]> get(byte[] key);

    void freezeCurMemTable();

    // TODO: 待 SST 模块实现后补充以下方法
    // void flushImmutableMemTable();
    // void sinkToPaimon();
    // void evictOldestSinkedSST();
    // void compactLocalSSTs();
    // void degradeMemCache();

    BucketStateSnapshot stateSnapshot();

    void close();
}
