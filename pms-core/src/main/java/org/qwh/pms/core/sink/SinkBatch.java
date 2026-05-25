package org.qwh.pms.core.sink;

import org.qwh.pms.core.storage.SSTMeta;

import java.util.List;

public record SinkBatch(
    String batchId,
    List<SSTMeta> ssts,
    long minSequenceId,
    long maxSequenceId
) {
    public SinkBatch {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        ssts = List.copyOf(ssts);
        if (ssts.isEmpty()) {
            throw new IllegalArgumentException("ssts must not be empty");
        }
        if (minSequenceId <= 0 || maxSequenceId <= 0 || minSequenceId > maxSequenceId) {
            throw new IllegalArgumentException("invalid sequence bounds");
        }
    }
}
