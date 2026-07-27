package org.qwh.pms.core.bucket;

import java.util.List;
import java.util.Optional;
import org.qwh.pms.core.bucket.operation.CompactionResult;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.EvictionResult;
import org.qwh.pms.core.bucket.operation.FlushResult;
import org.qwh.pms.core.bucket.operation.FreezeResult;
import org.qwh.pms.core.bucket.operation.SinkOperationResult;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Value;

public interface PMSBucketDirector {

    int MAX_WRITE_BATCH_COUNT = 1024;

    /** @throws PmsWriteOverloadedException if the maintenance backlog is already overloaded. */
    void put(byte[] key, byte[] value);

    /** @throws PmsWriteOverloadedException if the maintenance backlog is already overloaded. */
    void delete(byte[] key);

    /**
     * Writes the whole batch or rejects it before WAL append.
     *
     * @throws PmsWriteOverloadedException if the maintenance backlog is already overloaded
     */
    void writeBatch(List<WriteOp> ops);

    Optional<byte[]> get(byte[] key);

    Optional<Value> lookup(byte[] key);

    List<Entry> scan(byte[] startInclusive, Optional<byte[]> endExclusive);

    List<Entry> prefixScan(byte[] prefix);

    FreezeResult freezeCurMemTable();

    FlushResult flushImmutableMemTable();

    /** Executes one oldest continuous NEW prefix within the supplied sequence and batch bounds. */
    SinkOperationResult sinkToPaimon(SinkSelection selection);

    /**
     * Resumes the single recoverable Sink flight.
     *
     * <p>A prepared retry reuses the durable prepared payload. A finalizing retry only reapplies
     * local state derived from durable success metadata and never creates another Paimon commit.
     */
    SinkOperationResult resumeSinkFlight();

    /** Evicts one oldest SINKED run without performing an implicit compaction. */
    EvictionResult evictOldestSinkedSST();

    /** Compacts exactly one selected same-state continuous group; stale selections are noops. */
    CompactionResult compactLocalSSTs(CompactionSelection selection);

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
