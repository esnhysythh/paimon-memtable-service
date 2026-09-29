package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommittedLookupDeltaTest {
    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;
    private static final Comparator<InternalRow> COMPARATOR =
            new KeyComparatorSupplier(RowType.of(new IntType())).get();
    private static final LookupRequest REQUEST = LookupRequest.fullRow(key(5));

    @Test
    void missingAndInvalidBucketsDoNotBlockHealthyBuckets() throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        index.invalidate(PARTITION, 1);
        index.installSnapshot(PARTITION, 2, List.of());
        PaimonKeyValueLookupService service = service(index, new FileNameLookup());

        assertEquals(0, service.applyCommittedMessages(List.of(
                delta(0, List.of(), List.of(file("missing-bucket"))),
                delta(1, List.of(), List.of(file("invalid-bucket"))),
                delta(2, List.of(), List.of(file("new"))))));

        assertTrue(index.bucketView(PARTITION, 0).isEmpty());
        assertFalse(index.bucketView(PARTITION, 1).orElseThrow().valid());
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 1, REQUEST).kind());
        assertHit(service, 2, "new");
    }

    @Test
    void failedMiddleBucketDoesNotLoseEarlierOrLaterPublication() throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        for (int bucket = 0; bucket < 3; bucket++) {
            index.installSnapshot(PARTITION, bucket, List.of());
        }
        PaimonKeyValueLookupService service = service(index, new FileNameLookup());

        assertEquals(1, service.applyCommittedMessages(List.of(
                delta(0, List.of(), List.of(file("first"))),
                delta(1, List.of(file("not-live")), List.of(file("replacement"))),
                delta(2, List.of(), List.of(file("last"))))));

        assertHit(service, 0, "first");
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 1, REQUEST).kind());
        assertHit(service, 2, "last");
    }

    @Test
    void cacheCleanupAndSecondaryCleanupFailuresRemainBucketLocal() throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        DataFileMeta old = file("old");
        index.installSnapshot(PARTITION, 0, List.of(old));
        index.installSnapshot(PARTITION, 1, List.of());
        int[] cleanupCalls = {0};
        PaimonKeyValueLookupService service = service(index, new FileNameLookup() {
            @Override
            public void invalidate(FileLookupContext context, DataFileMeta file) {
                throw new IllegalStateException("per-file cleanup failed");
            }

            @Override
            public void invalidateBucket(FileLookupContext context) {
                cleanupCalls[0]++;
                assertFalse(index.bucketView(context.partition(), context.bucket()).orElseThrow().valid(),
                        "logical invalidation must precede fallible cleanup");
                throw new IllegalStateException("bucket cleanup also failed");
            }
        });

        assertEquals(1, service.applyCommittedMessages(List.of(
                delta(0, List.of(old), List.of(file("replacement"))),
                delta(1, List.of(), List.of(file("new"))))));

        assertEquals(1, cleanupCalls[0]);
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());
        assertHit(service, 1, "new");
    }

    @Test
    void invalidBucketSkipsLaterDeltasUntilCompleteSnapshotRestoresIt() throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        index.installSnapshot(PARTITION, 0, List.of());
        PaimonKeyValueLookupService service = service(index, new FileNameLookup());
        DataFileMeta first = file("first");
        DataFileMeta second = file("second");
        DataFileMeta third = file("third");

        assertEquals(1, service.applyCommittedMessages(List.of(
                delta(0, List.of(file("not-live")), List.of(first)))));
        assertEquals(0, service.applyCommittedMessages(List.of(delta(0, List.of(first), List.of(second)))));
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());

        service.installSnapshot(PARTITION, 0, List.of(second));
        assertHit(service, 0, "second");
        assertEquals(0, service.applyCommittedMessages(List.of(delta(0, List.of(second), List.of(third)))));
        assertHit(service, 0, "third");
    }

    @Test
    void tableWideInvalidationDefersCacheCleanupUntilEachSnapshotIsInstalled() throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        index.installSnapshot(PARTITION, 0, List.of(file("old")));
        index.installSnapshot(PARTITION, 1, List.of(file("other")));
        index.invalidate(PARTITION, 2);
        int[] cleanupCalls = {0};
        PaimonKeyValueLookupService service = service(index, new FileNameLookup() {
            @Override
            public void invalidateBucket(FileLookupContext context) {
                if (++cleanupCalls[0] == 1) {
                    throw new IllegalStateException("cache cleanup unavailable");
                }
            }
        });

        service.invalidateAllViews();
        assertEquals(0, cleanupCalls[0], "logical invalidation must not depend on cache IO");
        for (int bucket = 0; bucket < 3; bucket++) {
            assertTrue(index.bucketView(PARTITION, bucket).isEmpty());
            assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, bucket, REQUEST).kind());
        }
        assertEquals(0, service.applyCommittedMessages(List.of(delta(0, List.of(), List.of(file("delta-only"))))));
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());

        assertThrows(IllegalStateException.class,
                () -> service.installSnapshot(PARTITION, 0, List.of(file("snapshot"))));
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());
        service.installSnapshot(PARTITION, 0, List.of(file("snapshot")));
        assertHit(service, 0, "snapshot");
        assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 1, REQUEST).kind());
        assertEquals(2, cleanupCalls[0]);
    }

    @Test
    void failedLogicalInvalidationEscapesForTableWideRecovery() {
        RuntimeException failure = new IllegalStateException("cannot invalidate");
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4) {
            @Override
            public synchronized void invalidate(BinaryRow partition, int bucket) {
                throw failure;
            }
        };
        index.installSnapshot(PARTITION, 0, List.of());
        PaimonKeyValueLookupService service = service(index, new FileNameLookup());

        assertSame(failure, assertThrows(IllegalStateException.class, () -> service.applyCommittedMessages(
                List.of(delta(0, List.of(file("not-live")), List.of())))));
    }

    @Test
    void extractionFailureEscapesBeforeAnyBucketIsUpdated() {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        index.installSnapshot(PARTITION, 0, List.of());
        PaimonKeyValueLookupService service = service(index, new FileNameLookup());
        CommitMessage unsupported = new CommitMessage() {
            @Override
            public BinaryRow partition() { return PARTITION; }
            @Override
            public int bucket() { return 0; }
            @Override
            public Integer totalBuckets() { return 1; }
        };

        assertThrows(IllegalArgumentException.class, () -> service.applyCommittedMessages(List.of(
                delta(0, List.of(), List.of(file("new"))), unsupported)));
        assertTrue(index.bucketView(PARTITION, 0).orElseThrow().allFiles().isEmpty());
    }

    @Test
    void errorsAreNotSwallowedAsRecoverableBucketFailures() {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        DataFileMeta old = file("old");
        index.installSnapshot(PARTITION, 0, List.of(old));
        AssertionError failure = new AssertionError("fatal cleanup failure");
        PaimonKeyValueLookupService service = service(index, new FileNameLookup() {
            @Override
            public void invalidate(FileLookupContext context, DataFileMeta file) {
                throw failure;
            }
        });

        assertSame(failure, assertThrows(AssertionError.class, () -> service.applyCommittedMessages(
                List.of(delta(0, List.of(old), List.of(file("new")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidationDoesNotMutateCapturedViewAndNewLookupsReturnUnknown(boolean clearAll) throws Exception {
        LiveFileIndex index = new LiveFileIndex(COMPARATOR, 4);
        index.installSnapshot(PARTITION, 0, List.of(file("old")));
        var capturedView = index.bucketView(PARTITION, 0).orElseThrow();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PaimonKeyValueLookupService service = service(index, new FileNameLookup() {
            @Override
            public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request) {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return super.lookup(context, file, request);
            }
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var inFlight = executor.submit(() -> service.lookup(PARTITION, 0, REQUEST));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (clearAll) {
                service.invalidateAllViews();
            } else {
                assertEquals(1, service.applyCommittedMessages(List.of(
                        delta(0, List.of(file("not-live")), List.of(file("new"))))));
            }
            assertEquals(LookupResult.Kind.UNKNOWN, service.lookup(PARTITION, 0, REQUEST).kind());
            assertTrue(capturedView.valid());
            assertEquals("old", capturedView.allFiles().get(0).fileName());
            release.countDown();
            assertEquals("old", inFlight.get(5, TimeUnit.SECONDS).row().orElseThrow().getString(0).toString());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static PaimonKeyValueLookupService service(LiveFileIndex index, DataFileLookup lookup) {
        return new PaimonKeyValueLookupService(index, new CandidatePlanner(COMPARATOR, 0), lookup, 0);
    }

    private static void assertHit(PaimonKeyValueLookupService service, int bucket, String expected) throws Exception {
        LookupResult result = service.lookup(PARTITION, bucket, REQUEST);
        assertEquals(LookupResult.Kind.HIT, result.kind());
        assertEquals(expected, result.row().orElseThrow().getString(0).toString());
    }

    private static CommitMessage delta(int bucket, List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CommitMessageImpl(PARTITION, bucket, 3,
                new DataIncrement(after, before, List.of()), CompactIncrement.emptyIncrement());
    }

    private static DataFileMeta file(String name) {
        return DataFileMeta.create(name, 100L, 1L, key(0), key(9), null, null,
                0L, 0L, 0L, 1, 0L, null, null, null, null, null);
    }

    private static BinaryRow key(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private static class FileNameLookup implements DataFileLookup {
        @Override
        public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request) {
            return LookupResult.hit(GenericRow.of(BinaryString.fromString(file.fileName())), 0L);
        }

        @Override
        public void invalidate(FileLookupContext context, DataFileMeta file) {}
    }
}
