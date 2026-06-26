package org.qwh.pms.lookup.commit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.qwh.pms.lookup.view.PartitionBucket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Extracts latest-state data file deltas from already committed Paimon commit messages. */
public final class CommitMessageDeltaExtractor {

    public List<BucketFileDelta> extract(List<CommitMessage> messages) {
        Map<PartitionBucket, MutableFileDelta> deltas = new LinkedHashMap<>();
        for (CommitMessage message : messages) {
            if (!(message instanceof CommitMessageImpl committed)) {
                throw new IllegalArgumentException(
                        "Unsupported Paimon commit message: " + message.getClass().getName());
            }
            PartitionBucket bucket = PartitionBucket.of(committed.partition(), committed.bucket());
            MutableFileDelta delta = deltas.computeIfAbsent(bucket, ignored -> new MutableFileDelta());
            DataIncrement data = committed.newFilesIncrement();
            CompactIncrement compact = committed.compactIncrement();
            delta.before.addAll(data.deletedFiles());
            delta.before.addAll(compact.compactBefore());
            delta.after.addAll(data.newFiles());
            delta.after.addAll(compact.compactAfter());
        }

        List<BucketFileDelta> result = new ArrayList<>(deltas.size());
        for (Map.Entry<PartitionBucket, MutableFileDelta> entry : deltas.entrySet()) {
            PartitionBucket bucket = entry.getKey();
            MutableFileDelta delta = entry.getValue();
            result.add(new BucketFileDelta(
                    bucket.partition(),
                    bucket.bucket(),
                    List.copyOf(delta.before),
                    List.copyOf(delta.after)));
        }
        return result;
    }

    public record BucketFileDelta(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> afterFiles) {}

    private static final class MutableFileDelta {
        private final List<DataFileMeta> before = new ArrayList<>();
        private final List<DataFileMeta> after = new ArrayList<>();
    }
}
