package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupRequestValidator;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.LookupUnknownException;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.commit.CommitMessageDeltaExtractor;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveBucketView;
import org.qwh.pms.lookup.view.LiveFileIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** End-to-end latest-state lookup over PMS-owned live file metadata. */
public class PaimonKeyValueLookupService {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonKeyValueLookupService.class);

    private final LiveFileIndex fileIndex;
    private final CandidatePlanner candidatePlanner;
    private final DataFileLookup fileLookup;
    private final CommitMessageDeltaExtractor commitDeltaExtractor;
    private final long expectedSchemaId;

    public PaimonKeyValueLookupService(
            LiveFileIndex fileIndex,
            CandidatePlanner candidatePlanner,
            DataFileLookup fileLookup,
            long expectedSchemaId) {
        this.fileIndex = fileIndex;
        this.candidatePlanner = candidatePlanner;
        this.fileLookup = fileLookup;
        this.commitDeltaExtractor = new CommitMessageDeltaExtractor();
        this.expectedSchemaId = expectedSchemaId;
    }

    public LookupResult lookup(BinaryRow partition, int bucket, LookupRequest request)
            throws IOException {
        LookupRequestValidator.ensureNonNullKey(request);
        FileLookupContext context = new FileLookupContext(partition, bucket);
        Optional<LiveBucketView> maybeView = fileIndex.bucketView(partition, bucket);
        if (maybeView.isEmpty() || !maybeView.get().valid()) {
            LOG.debug(
                    "Paimon lookup view is not ready: partition={}, bucket={}, installed={}, valid={}",
                    partition,
                    bucket,
                    maybeView.isPresent(),
                    maybeView.map(LiveBucketView::valid).orElse(false));
            return LookupResult.unknown();
        }

        List<DataFileMeta> candidates = candidatePlanner.plan(maybeView.get(), request.key());
        LOG.debug(
                "Planned Paimon lookup candidates: partition={}, bucket={}, candidateCount={}",
                partition,
                bucket,
                candidates.size());
        for (DataFileMeta candidate : candidates) {
            ensureSchemaMatches(candidate);
            LookupResult result;
            try {
                result = fileLookup.lookup(context, candidate, request);
            } catch (LookupUnknownException | IOException e) {
                LOG.warn(
                        "Paimon lookup candidate failed: partition={}, bucket={}, file={}",
                        partition,
                        bucket,
                        candidate.fileName(),
                        e);
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
            LOG.warn(
                    "Failed to apply Paimon lookup delta; bucket invalidated: partition={}, bucket={}, beforeCount={}, afterCount={}",
                    partition,
                    bucket,
                    beforeFiles == null ? 0 : beforeFiles.size(),
                    afterFiles == null ? 0 : afterFiles.size(),
                    e);
            throw e;
        }
        if (beforeFiles != null) {
            beforeFiles.forEach(file -> fileLookup.invalidate(context, file));
        }
        LOG.info(
                "Applied Paimon lookup delta: partition={}, bucket={}, beforeCount={}, afterCount={}",
                partition,
                bucket,
                beforeFiles == null ? 0 : beforeFiles.size(),
                afterFiles == null ? 0 : afterFiles.size());
    }

    /** Installs a complete committed bucket snapshot before incremental deltas are applied. */
    public void installSnapshot(
            BinaryRow partition, int bucket, List<DataFileMeta> liveFiles) {
        fileLookup.invalidateBucket(new FileLookupContext(partition, bucket));
        fileIndex.installSnapshot(partition, bucket, liveFiles);
        LOG.info(
                "Installed Paimon lookup live file snapshot: partition={}, bucket={}, liveFileCount={}",
                partition,
                bucket,
                liveFiles == null ? 0 : liveFiles.size());
    }

    /**
     * Applies one already committed Paimon commit to initialized bucket views.
     *
     * <p>Uninitialized buckets intentionally skip the delta: their first lookup must install a
     * complete latest snapshot, which already includes this commit. An invalid bucket remains
     * invalid until that rebuild happens.
     */
    public void applyCommittedMessages(List<CommitMessage> messages) {
        for (CommitMessageDeltaExtractor.BucketFileDelta delta : commitDeltaExtractor.extract(messages)) {
            if (fileIndex.bucketView(delta.partition(), delta.bucket()).isEmpty()) {
                continue;
            }
            applyDelta(delta.partition(), delta.bucket(), delta.beforeFiles(), delta.afterFiles());
        }
    }

    private void ensureSchemaMatches(DataFileMeta file) {
        if (file.schemaId() != expectedSchemaId) {
            throw new SchemaMismatchException(file.fileName(), expectedSchemaId, file.schemaId());
        }
    }

}
