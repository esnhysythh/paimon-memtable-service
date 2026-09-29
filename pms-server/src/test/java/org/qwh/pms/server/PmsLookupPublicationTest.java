package org.qwh.pms.server;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.bucket.SinkFlightSnapshot;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.lookup.PaimonKeyValueLookupService;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.server.dev.PMSTestServer;
import org.qwh.pms.sink.paimon.PaimonCommitPayloadCodec;
import org.qwh.pms.sink.paimon.PaimonSinkManager;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsLookupPublicationTest {
    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;

    @TempDir
    Path tempDir;

    @Test
    void partialPublicationPreservesInsertsUpdatesAndDeletesAfterLocalEviction() throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            List<Integer> a = f.keysInBucket(0, 3);
            List<Integer> b = f.keysInBucket(1, 3);
            // Initialize both views before the first commit, so all publication is incremental.
            assertTrue(f.service.get(Map.of("id", a.get(0))).isEmpty());
            assertTrue(f.service.get(Map.of("id", b.get(0))).isEmpty());
            for (List<Integer> keys : List.of(a, b)) {
                f.write(keys.get(0), "old-update");
                f.write(keys.get(1), "old-delete");
            }
            f.flush();
            f.sink();
            f.evict();

            // The Paimon commit is real; corrupt only A's published delta, before B in the payload.
            f.sinkManager.payloadTransform = PmsLookupPublicationTest::withInvalidFirstBucketDelta;
            for (List<Integer> keys : List.of(a, b)) {
                f.write(keys.get(0), "updated");
                f.service.delete(Map.of("id", keys.get(1)));
                f.write(keys.get(2), "inserted");
            }
            f.flush();
            f.sink();
            assertFalse(f.index.bucketView(PARTITION, 0).orElseThrow().valid());
            assertTrue(f.index.bucketView(PARTITION, 1).orElseThrow().valid());
            assertEquals(SinkFlightSnapshot.Status.IDLE, f.service.stateSnapshot().sinkFlight().status());
            assertFalse(f.service.sinkToPaimon(new SinkSelection(Long.MAX_VALUE, Long.MAX_VALUE)).progressed());
            assertEquals(2, f.sinkManager.commitCalls);
            f.evict();
            assertEquals(0, f.service.stateSnapshot().sinkedSSTCount());

            // Read B first: a stale valid view would return old values or miss the insert.
            for (List<Integer> keys : List.of(b, a)) {
                f.assertHit(keys.get(0), "updated");
                f.assertHit(keys.get(2), "inserted");
                assertTrue(f.service.get(Map.of("id", keys.get(1))).isEmpty());
                assertNotEquals(LookupResultType.HIT, f.service.getFullRaw(f.rawKey(keys.get(1))).type());
            }
            assertTrue(f.index.bucketView(PARTITION, 0).orElseThrow().valid());

            f.sinkManager.payloadTransform = UnaryOperator.identity();
            f.write(a.get(0), "after-rebuild");
            f.flush();
            f.sink();
            f.evict();
            f.assertHit(a.get(0), "after-rebuild");
            assertEquals(3, f.sinkManager.prepareCalls);
            assertEquals(3, f.sinkManager.commitCalls);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void decodeFailureClearsAllViewsAndRebuildsLatestSnapshotWithoutRestart(boolean rawFirst) throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            List<Integer> a = f.keysInBucket(0, 3);
            List<Integer> b = f.keysInBucket(1, 3);
            for (List<Integer> keys : List.of(a, b)) {
                f.write(keys.get(0), "old-update");
                f.write(keys.get(1), "old-delete");
            }
            f.flush();
            f.sink();
            f.evict();
            f.assertHit(a.get(0), "old-update");
            f.assertHit(b.get(0), "old-update");

            f.sinkManager.payloadTransform = ignored -> new byte[] {1};
            for (List<Integer> keys : List.of(a, b)) {
                f.write(keys.get(0), "updated");
                f.service.delete(Map.of("id", keys.get(1)));
                f.write(keys.get(2), "inserted");
            }
            f.flush();
            f.sink();
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            assertTrue(f.index.bucketView(PARTITION, 1).isEmpty());
            f.assertHit(a.get(0), "updated"); // Local hits do not rebuild the cleared views.
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            f.evict();

            // A later delta must leave missing views missing. Rebuild includes both commits.
            f.sinkManager.payloadTransform = UnaryOperator.identity();
            for (List<Integer> keys : List.of(a, b)) {
                f.write(keys.get(0), "latest");
            }
            f.flush();
            f.sink();
            f.evict();
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            assertTrue(f.index.bucketView(PARTITION, 1).isEmpty());
            for (List<Integer> keys : List.of(a, b)) {
                if (rawFirst) {
                    f.assertRawHit(keys.get(0), "latest");
                }
                f.assertHit(keys.get(0), "latest");
                f.assertHit(keys.get(2), "inserted");
                assertTrue(f.service.get(Map.of("id", keys.get(1))).isEmpty());
                LookupResultType deleted = f.service.getFullRaw(f.rawKey(keys.get(1))).type();
                assertTrue(deleted == LookupResultType.DELETED || deleted == LookupResultType.MISS);
            }
            assertFalse(f.service.sinkToPaimon(new SinkSelection(Long.MAX_VALUE, Long.MAX_VALUE)).progressed());
            assertEquals(3, f.sinkManager.commitCalls);
        }
        try (Fixture reopened = new Fixture(tempDir)) {
            for (int bucket = 0; bucket < 2; bucket++) {
                reopened.assertHit(reopened.keysInBucket(bucket, 1).get(0), "latest");
            }
            assertEquals(0, reopened.sinkManager.commitCalls, "restart must not recommit durable success");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uncontainedFailureAfterPartialPublicationClearsBothUpdatedAndStaleViews(boolean errorFailure) throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            int a = f.keysInBucket(0, 1).get(0);
            int b = f.keysInBucket(1, 1).get(0);
            f.write(a, "old");
            f.write(b, "old");
            f.flush();
            f.sink();
            f.evict();
            f.assertHit(a, "old");
            f.assertHit(b, "old");
            Throwable failure = errorFailure ? new AssertionError("error after publishing the first bucket")
                    : new IllegalStateException("failure after publishing the first bucket");
            AtomicBoolean failOnce = new AtomicBoolean(true);
            f.replaceLookup(new PaimonKeyValueLookupService(
                    f.index, new CandidatePlanner(f.comparator, 0), f.fileLookup,
                    f.table.schema().id()) {
                @Override
                public int applyCommittedMessages(List<CommitMessage> messages) {
                    if (failOnce.getAndSet(false)) {
                        super.applyCommittedMessages(messages.stream().filter(m -> m.bucket() == 0).toList());
                        if (failure instanceof Error error) {
                            throw error;
                        }
                        throw (RuntimeException) failure;
                    }
                    return super.applyCommittedMessages(messages);
                }
            });
            f.write(a, "new");
            f.write(b, "new");
            f.flush();
            if (errorFailure) {
                assertSame(failure, assertThrows(AssertionError.class, f::sink));
            } else {
                f.sink();
            }
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            assertTrue(f.index.bucketView(PARTITION, 1).isEmpty());
            assertEquals(SinkFlightSnapshot.Status.IDLE, f.service.stateSnapshot().sinkFlight().status());
            assertFalse(f.service.sinkToPaimon(new SinkSelection(Long.MAX_VALUE, Long.MAX_VALUE)).progressed());
            assertEquals(2, f.sinkManager.commitCalls);
            f.evict();
            f.assertRawHit(b, "new");
            f.assertHit(a, "new");

            f.write(a, "after-rebuild");
            f.write(b, "after-rebuild");
            f.flush();
            f.sink();
            f.evict();
            f.assertHit(a, "after-rebuild");
            f.assertHit(b, "after-rebuild");
            assertEquals(3, f.sinkManager.commitCalls);
        }
    }

    @Test
    void failedSnapshotCacheCleanupRemainsUnavailableAndCanRetry() throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            int id = f.keysInBucket(0, 1).get(0);
            assertTrue(f.service.get(Map.of("id", id)).isEmpty());
            f.sinkManager.payloadTransform = ignored -> new byte[] {1};
            f.write(id, "committed");
            f.flush();
            f.sink();
            f.evict();
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            RuntimeException failure = new IllegalStateException("snapshot cache cleanup failed");
            DataFileLookup flakyLookup = new DelegatingLookup(f.fileLookup) {
                private int failuresRemaining = 2;
                @Override
                public void invalidateBucket(FileLookupContext context) {
                    if (failuresRemaining-- > 0) {
                        throw failure;
                    }
                    super.invalidateBucket(context);
                }
            };
            f.replaceLookup(new PaimonKeyValueLookupService(
                    f.index, new CandidatePlanner(f.comparator, 0), flakyLookup,
                    f.table.schema().id()));

            assertSame(failure, f.assertHistoryUnavailable(id).getCause());
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            f.assertHit(id, "committed");
            assertTrue(f.index.bucketView(PARTITION, 0).orElseThrow().valid());
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void waitingRebuildRecoversAfterPublicationFailureWhileHealthyLookupDoesNotTakeCommitLock(boolean raw, boolean errorFailure)
            throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            List<Integer> a = f.keysInBucket(0, 2);
            int missing = f.keysInBucket(1, 1).get(0);
            f.write(a.get(0), "history");
            f.write(missing, "other-bucket");
            f.flush();
            f.sink();
            f.evict();
            f.assertHit(a.get(0), "history"); // Only bucket 0 is initialized.

            CountDownLatch publishing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Throwable failure = errorFailure ? new AssertionError("publication error")
                    : new IllegalStateException("uncontained publication failure");
            f.replaceLookup(new PaimonKeyValueLookupService(
                    f.index, new CandidatePlanner(f.comparator, 0), f.fileLookup,
                    f.table.schema().id()) {
                @Override
                public int applyCommittedMessages(List<CommitMessage> messages) {
                    publishing.countDown();
                    await(release);
                    if (failure instanceof Error error) {
                        throw error;
                    }
                    throw (RuntimeException) failure;
                }
            });
            f.write(a.get(1), "new");
            f.flush();
            AtomicReference<Throwable> sinkFailure = new AtomicReference<>();
            AtomicReference<Throwable> readFailure = new AtomicReference<>();
            Thread sink = new Thread(() -> {
                try { f.sink(); } catch (Throwable e) { sinkFailure.set(e); }
            });
            Thread read = new Thread(() -> {
                try {
                    if (raw) { f.assertRawHit(missing, "other-bucket"); }
                    else {
                        assertEquals(Map.of("id", missing, "marker", "other-bucket"),
                                f.service.get(Map.of("id", missing)).orElseThrow());
                    }
                } catch (Throwable e) { readFailure.set(e); }
            });
            var healthyReader = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                sink.start();
                assertTrue(publishing.await(5, TimeUnit.SECONDS));
                healthyReader.submit(() -> f.assertHit(a.get(0), "history")).get(5, TimeUnit.SECONDS);
                read.start();
                awaitBlocked(read);
                release.countDown();
                sink.join(5_000);
                read.join(5_000);
                assertFalse(sink.isAlive());
                assertFalse(read.isAlive());
                if (errorFailure) {
                    assertSame(failure, sinkFailure.get());
                } else {
                    assertNull(sinkFailure.get());
                }
                assertNull(readFailure.get());
                assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
                assertTrue(f.index.bucketView(PARTITION, 1).orElseThrow().valid());
            } finally {
                release.countDown();
                sink.join(5_000);
                read.join(5_000);
                healthyReader.shutdownNow();
            }
        }
    }

    @Test
    void snapshotRebuildAndNextCommitRemainOrdered() throws Exception {
        try (Fixture f = new Fixture(tempDir)) {
            int id = f.keysInBucket(0, 1).get(0);
            f.sinkManager.payloadTransform = ignored -> new byte[] {1};
            f.write(id, "old");
            f.flush();
            f.sink();
            f.evict();
            assertTrue(f.index.bucketView(PARTITION, 0).isEmpty());
            f.sinkManager.payloadTransform = UnaryOperator.identity();
            CountDownLatch rebuilding = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            DataFileLookup blockingLookup = new DelegatingLookup(f.fileLookup) {
                @Override
                public void invalidateBucket(FileLookupContext context) {
                    rebuilding.countDown();
                    await(release);
                    super.invalidateBucket(context);
                }
            };
            f.replaceLookup(new PaimonKeyValueLookupService(
                    f.index, new CandidatePlanner(f.comparator, 0), blockingLookup,
                    f.table.schema().id()));
            AtomicReference<Throwable> readFailure = new AtomicReference<>();
            AtomicReference<Throwable> sinkFailure = new AtomicReference<>();
            Thread read = new Thread(() -> {
                try {
                    assertEquals(Map.of("id", id, "marker", "old"),
                            f.service.get(Map.of("id", id)).orElseThrow());
                } catch (Throwable e) { readFailure.set(e); }
            });
            Thread sink = new Thread(() -> {
                try { f.sink(); } catch (Throwable e) { sinkFailure.set(e); }
            });
            try {
                read.start();
                assertTrue(rebuilding.await(5, TimeUnit.SECONDS));
                f.write(id, "new"); // Foreground writes and local flush do not take the table lock.
                f.flush();
                sink.start();
                awaitBlocked(sink);
                assertEquals(1, f.sinkManager.commitCalls);
                release.countDown();
                read.join(5_000);
                sink.join(5_000);
                assertFalse(read.isAlive());
                assertFalse(sink.isAlive());
                assertNull(readFailure.get());
                assertNull(sinkFailure.get());
                assertTrue(f.index.bucketView(PARTITION, 0).orElseThrow().valid());
                f.evict();
                f.assertHit(id, "new");
                assertEquals(2, f.sinkManager.commitCalls);
            } finally {
                release.countDown();
                read.join(5_000);
                sink.join(5_000);
            }
        }
    }

    private static byte[] withInvalidFirstBucketDelta(byte[] payload) {
        PaimonCommitPayloadCodec codec = new PaimonCommitPayloadCodec();
        List<CommitMessage> messages = new ArrayList<>(codec.decode(payload));
        messages.sort(Comparator.comparingInt(CommitMessage::bucket));
        CommitMessageImpl first = (CommitMessageImpl) messages.get(0);
        assertEquals(0, first.bucket());
        DataIncrement increment = first.newFilesIncrement();
        List<DataFileMeta> removed = new ArrayList<>(increment.deletedFiles());
        removed.add(increment.newFiles().get(0)); // Not live in the preceding snapshot.
        messages.set(0, new CommitMessageImpl(first.partition(), first.bucket(), first.totalBuckets(),
                new DataIncrement(increment.newFiles(), removed, increment.changelogFiles()), first.compactIncrement()));
        return codec.encode(messages);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertEquals(Thread.State.BLOCKED, thread.getState());
    }

    // Keep fault injection in tests; no production-only factories or recovery abstractions are needed.
    private static Field field(Class<?> type, String name) throws ReflectiveOperationException {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static final class Fixture implements AutoCloseable {
        final PMSTestServer server;
        final PmsTableService service;
        final FileStoreTable table;
        final Comparator<InternalRow> comparator;
        final LiveFileIndex index;
        final DataFileLookup fileLookup;
        final RecordingSinkManager sinkManager;
        final PmsPrimaryKeyCodec keyCodec;

        Fixture(Path root) throws Exception {
            Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).column("marker", DataTypes.STRING())
                    .primaryKey("id").option("bucket", "2").option("file.format", "parquet")
                    .option("merge-engine", "deduplicate").build();
            server = PMSTestServer.create(root, schema);
            table = (FileStoreTable) server.table();
            comparator = ((KeyValueFileStore) table.store()).newKeyComparator();
            AtomicReference<RecordingSinkManager> manager = new AtomicReference<>();
            service = PmsTableService.open(server.config(), (loaded, user, storage) -> {
                RecordingSinkManager recording = new RecordingSinkManager(new PaimonSinkManager(loaded, user, storage));
                manager.set(recording);
                return recording;
            });
            sinkManager = manager.get();
            PaimonKeyValueLookupService lookup = (PaimonKeyValueLookupService)
                    field(PmsTableService.class, "paimonLookup").get(service);
            index = (LiveFileIndex) field(PaimonKeyValueLookupService.class, "fileIndex").get(lookup);
            fileLookup = (DataFileLookup) field(PaimonKeyValueLookupService.class, "fileLookup").get(lookup);
            keyCodec = PmsPrimaryKeyCodec.forFieldNames(table.rowType(), List.of("id"));
        }

        void replaceLookup(PaimonKeyValueLookupService lookup) throws Exception {
            field(PmsTableService.class, "paimonLookup").set(service, lookup);
        }

        List<Integer> keysInBucket(int bucket, int count) {
            List<Integer> keys = new ArrayList<>();
            var extractor = table.createRowKeyExtractor();
            for (int id = 0; keys.size() < count && id < 10_000; id++) {
                extractor.setRecord(GenericRow.of(id, BinaryString.fromString("")));
                if (extractor.bucket() == bucket) { keys.add(id); }
            }
            assertEquals(count, keys.size());
            return keys;
        }

        byte[] rawKey(int id) { return keyCodec.encodeKeyTuple(GenericRow.of(id)); }
        void write(int id, String marker) { service.write(Map.of("id", id, "marker", marker)); }
        void flush() { service.freezeCurMemTable(); service.flushImmutableMemTable(); }
        void sink() { assertTrue(service.sinkToPaimon(new SinkSelection(Long.MAX_VALUE, Long.MAX_VALUE)).progressed()); }
        void evict() { assertTrue(service.evictOldestSinkedSST().progressed()); }

        void assertHit(int id, String marker) {
            assertEquals(Map.of("id", id, "marker", marker), service.get(Map.of("id", id)).orElseThrow());
            assertRawHit(id, marker);
        }

        void assertRawHit(int id, String marker) {
            var raw = service.getFullRaw(rawKey(id));
            assertEquals(LookupResultType.HIT, raw.type());
            assertEquals(marker, new PmsRowValueCodec().decode(table.rowType(), raw.row()).getString(1).toString());
        }

        PmsLookupUnavailableException assertHistoryUnavailable(int id) {
            PmsLookupUnavailableException json = assertThrows(PmsLookupUnavailableException.class,
                    () -> service.get(Map.of("id", id)));
            assertThrows(PmsLookupUnavailableException.class, () -> service.getFullRaw(rawKey(id)));
            return json;
        }

        @Override
        public void close() throws Exception {
            try { service.close(); } finally { server.close(); }
        }
    }

    private static final class RecordingSinkManager implements SinkManager {
        final SinkManager delegate;
        UnaryOperator<byte[]> payloadTransform = UnaryOperator.identity();
        volatile int prepareCalls;
        volatile int commitCalls;

        RecordingSinkManager(SinkManager delegate) { this.delegate = delegate; }
        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) { prepareCalls++; return delegate.prepare(batch); }
        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            commitCalls++;
            SinkCommitResult result = delegate.commit(prepared);
            return new SinkCommitResult(result.batchId(), result.snapshotId(), result.persistedSequenceId(),
                    result.sstIds(), payloadTransform.apply(result.commitPayload()));
        }
    }

    private static class DelegatingLookup implements DataFileLookup {
        final DataFileLookup delegate;
        DelegatingLookup(DataFileLookup delegate) { this.delegate = delegate; }
        @Override
        public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request) throws IOException {
            return delegate.lookup(context, file, request);
        }
        @Override
        public void invalidate(FileLookupContext context, DataFileMeta file) { delegate.invalidate(context, file); }
        @Override
        public void invalidateBucket(FileLookupContext context) { delegate.invalidateBucket(context); }
    }
}
