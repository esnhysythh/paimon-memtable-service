package org.qwh.pms.core.bucket;

import java.util.Optional;
import java.util.List;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.storage.SSTMeta;

public interface PMSBucketDirector {

    void put(byte[] key, byte[] value);

    void delete(byte[] key);

    void writeBatch(List<WriteOp> ops);

    Optional<byte[]> get(byte[] key);

    Optional<Value> lookup(byte[] key);

    List<Entry> scan(byte[] startInclusive, Optional<byte[]> endExclusive);

    List<Entry> prefixScan(byte[] prefix);

    void freezeCurMemTable();

    void flushImmutableMemTable();

    Optional<SinkCommitResult> sinkToPaimon();

    Optional<SSTMeta> evictOldestSinkedSST();

    void compactLocalSSTs();

    // TODO: 待 Mem 缓存退化模块实现后补充以下方法
    // void degradeMemCache();

    BucketStateSnapshot stateSnapshot();

    void close();

    record WriteOp(byte[] key, byte[] value) {

        public WriteOp {
            if (key == null) {
                throw new NullPointerException("key must not be null");
            }
            if (key.length == 0) {
                throw new IllegalArgumentException("key must not be empty");
            }
        }

        public static WriteOp put(byte[] key, byte[] value) {
            if (value == null) {
                throw new NullPointerException("value must not be null; use delete(key) for tombstones");
            }
            return new WriteOp(key, value);
        }

        public static WriteOp delete(byte[] key) {
            return new WriteOp(key, null);
        }

        public boolean isDelete() {
            return value == null;
        }
    }
}
