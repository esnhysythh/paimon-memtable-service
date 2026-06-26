package org.qwh.pms.lookup.view;

import org.apache.paimon.data.BinaryRow;

import java.util.Objects;

/** Identifies one Paimon partition-bucket file view. */
public final class PartitionBucket {

    private final BinaryRow partition;
    private final int bucket;

    private PartitionBucket(BinaryRow partition, int bucket) {
        this.partition = partition.copy();
        this.bucket = bucket;
    }

    public static PartitionBucket of(BinaryRow partition, int bucket) {
        return new PartitionBucket(partition, bucket);
    }

    public BinaryRow partition() {
        return partition.copy();
    }

    public int bucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionBucket that)) {
            return false;
        }
        return bucket == that.bucket && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket);
    }

    @Override
    public String toString() {
        return "PartitionBucket{" + "partition=" + partition + ", bucket=" + bucket + '}';
    }
}
