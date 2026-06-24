package org.qwh.pms.lookup.api;

import org.apache.paimon.data.BinaryRow;

import java.util.Objects;

/** Stable partition-bucket context for one candidate data-file lookup. */
public final class FileLookupContext {

    private final BinaryRow partition;
    private final int bucket;

    public FileLookupContext(BinaryRow partition, int bucket) {
        this.partition = Objects.requireNonNull(partition, "partition").copy();
        this.bucket = bucket;
    }

    public BinaryRow partition() {
        return partition.copy();
    }

    public int bucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FileLookupContext that)) {
            return false;
        }
        return bucket == that.bucket && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket);
    }
}
