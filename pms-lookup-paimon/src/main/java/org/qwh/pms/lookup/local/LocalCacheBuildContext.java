package org.qwh.pms.lookup.local;

import org.apache.paimon.data.BinaryRow;

import java.util.Objects;

/** Per-build context shared by local cache modes. */
public record LocalCacheBuildContext(BinaryRow partition, int bucket) {

    public LocalCacheBuildContext {
        partition = Objects.requireNonNull(partition, "partition").copy();
    }

    @Override
    public BinaryRow partition() {
        return partition.copy();
    }
}
