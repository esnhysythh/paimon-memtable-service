package org.qwh.pms.lookup.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupRequestValidator;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.LookupUnknownException;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.live.CandidatePlanner;
import org.qwh.pms.lookup.live.LiveBucketView;
import org.qwh.pms.lookup.live.LiveFileIndex;
import org.qwh.pms.lookup.live.PartitionBucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** End-to-end latest-state lookup over PMS-owned live file metadata. */
public class PaimonKeyValueLookupService {

    private final LiveFileIndex fileIndex;
    private final CandidatePlanner candidatePlanner;
    private final DataFileLookup fileLookup;
    private final long expectedSchemaId;

    public PaimonKeyValueLookupService(
            LiveFileIndex fileIndex,
            CandidatePlanner candidatePlanner,
            DataFileLookup fileLookup,
            long expectedSchemaId) {
        this.fileIndex = fileIndex;
        this.candidatePlanner = candidatePlanner;
        this.fileLookup = fileLookup;
        this.expectedSchemaId = expectedSchemaId;
    }

    public LookupResult lookup(BinaryRow partition, int bucket, LookupRequest request)
            throws IOException {
        LookupRequestValidator.ensureNonNullKey(request);
        FileLookupContext context = new FileLookupContext(partition, bucket);
        Optional<LiveBucketView> maybeView = fileIndex.bucketView(partition, bucket);
        if (maybeView.isEmpty() || !maybeView.get().valid()) {
            return LookupResult.unknown();
        }

        List<DataFileMeta> candidates = candidatePlanner.plan(maybeView.get(), request.key());
        for (DataFileMeta candidate : candidates) {
            ensureSchemaMatches(candidate);
            LookupResult result;
            try {
                result = fileLookup.lookup(context, candidate, request);
            } catch (LookupUnknownException | IOException e) {
                return LookupResult.unknown();
            }
            if (result.kind() == LookupResult.Kind.HIT
                    || result.kind() == LookupResult.Kind.DELETED
                    || result.kind() == LookupResult.Kind.UNKNOWN) {
                return result;
            }
        }
        return LookupResult.miss();
    }

    public void applyDelta(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> afterFiles) {
        FileLookupContext context = new FileLookupContext(partition, bucket);
        try {
            fileIndex.applyDelta(partition, bucket, beforeFiles, afterFiles);
        } catch (RuntimeException e) {
            fileLookup.invalidateBucket(context);
            throw e;
        }
        if (beforeFiles != null) {
            beforeFiles.forEach(file -> fileLookup.invalidate(context, file));
        }
    }

    /** Installs a complete committed bucket snapshot before incremental deltas are applied. */
    public void installSnapshot(
            BinaryRow partition, int bucket, List<DataFileMeta> liveFiles) {
        fileLookup.invalidateBucket(new FileLookupContext(partition, bucket));
        fileIndex.installSnapshot(partition, bucket, liveFiles);
    }

    /**
     * Applies one already committed Paimon commit to initialized bucket views.
     *
     * <p>Uninitialized buckets intentionally skip the delta: their first lookup must install a
     * complete latest snapshot, which already includes this commit. An invalid bucket remains
     * invalid until that rebuild happens.
     */
    public void applyCommittedMessages(List<CommitMessage> messages) {
        Map<PartitionBucket, FileDelta> deltas = new LinkedHashMap<>();
        for (CommitMessage message : messages) {
            if (!(message instanceof CommitMessageImpl committed)) {
                throw new IllegalArgumentException(
                        "Unsupported Paimon commit message: " + message.getClass().getName());
            }
            PartitionBucket bucket = PartitionBucket.of(committed.partition(), committed.bucket());
            FileDelta delta = deltas.computeIfAbsent(bucket, ignored -> new FileDelta());
            DataIncrement data = committed.newFilesIncrement();
            CompactIncrement compact = committed.compactIncrement();
            delta.before.addAll(data.deletedFiles());
            delta.before.addAll(compact.compactBefore());
            delta.after.addAll(data.newFiles());
            delta.after.addAll(compact.compactAfter());
        }

        for (Map.Entry<PartitionBucket, FileDelta> entry : deltas.entrySet()) {
            PartitionBucket bucket = entry.getKey();
            if (fileIndex.bucketView(bucket.partition(), bucket.bucket()).isEmpty()) {
                continue;
            }
            FileDelta delta = entry.getValue();
            applyDelta(bucket.partition(), bucket.bucket(), delta.before, delta.after);
        }
    }

    private void ensureSchemaMatches(DataFileMeta file) {
        if (file.schemaId() != expectedSchemaId) {
            throw new SchemaMismatchException(file.fileName(), expectedSchemaId, file.schemaId());
        }
    }

    private static final class FileDelta {
        private final List<DataFileMeta> before = new ArrayList<>();
        private final List<DataFileMeta> after = new ArrayList<>();
    }
}
