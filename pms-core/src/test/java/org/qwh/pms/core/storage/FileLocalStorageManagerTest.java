package org.qwh.pms.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FileLocalStorageManagerTest {

    @TempDir
    Path tempDir;

    private FileLocalStorageManager storage() throws IOException {
        FileLocalStorageManager storage = new FileLocalStorageManager(
            new StorageConfig(tempDir.toString(), 0, 0, 0, 0)
        );
        storage.init();
        return storage;
    }

    private ImmutableMemTable immutable(Object... triples) {
        SkipListCurMemTable cur = new SkipListCurMemTable(new MemTableConfig(1_000_000, 256));
        for (int i = 0; i < triples.length; i += 3) {
            String key = (String) triples[i];
            byte[] value = (byte[]) triples[i + 1];
            long sequenceId = (long) triples[i + 2];
            cur.put(
                new Key(key.getBytes()),
                value == null ? Value.tombstone(sequenceId) : new Value(value, sequenceId)
            );
        }
        return cur.freeze();
    }

    private Optional<Value> get(FileLocalStorageManager storage, SSTMeta meta, String key) {
        try (SSTReadSnapshot leases = storage.readSnapshot(List.of(meta))) {
            return leases.get(meta, new Key(key.getBytes()));
        }
    }

    @Test
    void flushAndReadPut() throws IOException {
        FileLocalStorageManager storage = storage();
        ImmutableMemTable memTable = immutable(
            "k1", "v1".getBytes(), 1L,
            "k2", "v2".getBytes(), 2L
        );

        SSTMeta meta = storage.flushToSST(memTable);
        Optional<Value> value = get(storage, meta, "k2");

        assertTrue(value.isPresent());
        assertFalse(value.get().isTombstone());
        assertArrayEquals("v2".getBytes(), value.get().bytes());
        assertEquals(2L, value.get().sequenceId());
    }

    @Test
    void flushPreparationDoesNotBlockTheVisibleSstView() throws Exception {
        FileLocalStorageManager storage = storage();
        SSTMeta existing = storage.flushToSST(immutable("existing", "v0".getBytes(), 1L));
        CountDownLatch iteratorEntered = new CountDownLatch(1);
        CountDownLatch allowIterator = new CountDownLatch(1);
        ImmutableMemTable blocked = blockIterator(
            immutable("new", "v1".getBytes(), 2L),
            iteratorEntered,
            allowIterator
        );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<SSTMeta> flush = executor.submit(() -> storage.flushToSST(blocked));
        try {
            assertTrue(iteratorEntered.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                assertEquals(List.of(existing.runId()),
                    storage.metas().stream().map(SSTMeta::runId).toList());
                try (SSTReadSnapshot snapshot = storage.readVisibleSnapshot()) {
                    assertEquals(List.of(existing.runId()),
                        snapshot.metas().stream().map(SSTMeta::runId).toList());
                    assertArrayEquals(
                        "v0".getBytes(),
                        snapshot.get(existing, new Key("existing".getBytes())).orElseThrow().bytes()
                    );
                }
            });
            assertFalse(flush.isDone());
            allowIterator.countDown();

            SSTMeta published = flush.get(5, TimeUnit.SECONDS);
            assertEquals(
                List.of(existing.runId(), published.runId()),
                storage.metas().stream().map(SSTMeta::runId).toList()
            );
        } finally {
            allowIterator.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void missReturnsEmpty() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        assertTrue(get(storage, meta, "missing").isEmpty());
    }

    @Test
    void tombstoneReturnsValueWithNullBytes() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", null, 7L));

        Optional<Value> value = get(storage, meta, "k1");

        assertTrue(value.isPresent());
        assertTrue(value.get().isTombstone());
        assertEquals(7L, value.get().sequenceId());
    }

    @Test
    void iteratorScansEntriesInKeyOrder() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable(
            "k2", "v2".getBytes(), 2L,
            "k1", "v1".getBytes(), 1L,
            "k3", null, 3L
        ));

        List<String> keys = new ArrayList<>();
        List<Long> sequences = new ArrayList<>();
        List<Boolean> tombstones = new ArrayList<>();
        try (SSTReadSnapshot leases = storage.readSnapshot(List.of(meta))) {
            SSTEntryIterator iterator = leases.openIterator(meta);
            while (iterator.hasNext()) {
                var entry = iterator.next();
                keys.add(new String(entry.key().bytes()));
                sequences.add(entry.value().sequenceId());
                tombstones.add(entry.value().isTombstone());
            }
        }

        assertEquals(List.of("k1", "k2", "k3"), keys);
        assertEquals(List.of(1L, 2L, 3L), sequences);
        assertEquals(List.of(false, false, true), tombstones);
    }

    @Test
    void openIteratorKeepsReaderAliveUntilClosed() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable(
            "k1", "v1".getBytes(), 1L,
            "k2", "v2".getBytes(), 2L
        ));

        List<String> keys = new ArrayList<>();
        try (SSTReadSnapshot leases = storage.readSnapshot(List.of(meta))) {
            storage.deleteSST(meta);

            assertTrue(storage.metas().isEmpty());
            assertTrue(Files.exists(meta.path()));
            SSTEntryIterator iterator = leases.openIterator(meta);
            while (iterator.hasNext()) {
                keys.add(new String(iterator.next().key().bytes()));
            }
        }

        assertEquals(List.of("k1", "k2"), keys);
        assertFalse(Files.exists(meta.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
    }

    @Test
    void readSnapshotKeepsAllReadersAliveUntilClosed() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("k2", "v2".getBytes(), 2L));

        try (SSTReadSnapshot leases = storage.readSnapshot(List.of(first, second))) {
            storage.deleteSST(first);
            storage.deleteSST(second);

            assertTrue(storage.metas().isEmpty());
            assertTrue(Files.exists(first.path()));
            assertTrue(Files.exists(second.path()));
            assertArrayEquals(
                "v1".getBytes(),
                leases.get(first, new Key("k1".getBytes())).orElseThrow().bytes()
            );
            assertArrayEquals(
                "v2".getBytes(),
                leases.get(second, new Key("k2".getBytes())).orElseThrow().bytes()
            );
        }

        assertFalse(Files.exists(first.path()));
        assertFalse(Files.exists(second.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
        assertFalse(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
    }

    @Test
    void rangeIteratorScansOnlyRequestedKeyRange() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable(
            "a", "va".getBytes(), 1L,
            "b", "vb".getBytes(), 2L,
            "c", "vc".getBytes(), 3L,
            "d", "vd".getBytes(), 4L
        ));

        List<String> keys = new ArrayList<>();
        try (SSTReadSnapshot leases = storage.readSnapshot(List.of(meta))) {
            SSTEntryIterator iterator = leases.openIterator(
                meta,
                new Key("b".getBytes()),
                Optional.of(new Key("d".getBytes()))
            );
            while (iterator.hasNext()) {
                keys.add(new String(iterator.next().key().bytes()));
            }
        }

        assertEquals(List.of("b", "c"), keys);
    }

    @Test
    void metaCarriesKeyAndSequenceBoundaries() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable(
            "a", "v1".getBytes(), 11L,
            "z", "v2".getBytes(), 12L
        ));

        assertEquals(2, meta.entryCount());
        assertArrayEquals("a".getBytes(), meta.minKey().bytes());
        assertArrayEquals("z".getBytes(), meta.maxKey().bytes());
        assertEquals(11L, meta.minSequenceId());
        assertEquals(12L, meta.maxSequenceId());
        assertTrue(meta.oldestWriteAtMillis() > 0);
        assertTrue(Files.exists(meta.path()));
        assertTrue(meta.fileSize() > SSTFormat.FOOTER_SIZE);
    }

    @Test
    void flushWritesHumanReadableSstMetadata() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        Path metaPath = tempDir.resolve("sst-000001-000001.meta.json");
        String content = Files.readString(metaPath);

        assertTrue(Files.exists(metaPath));
        assertTrue(content.contains("\"version\": 2"));
        assertTrue(content.contains("\"runId\": " + meta.runId()));
        assertTrue(content.contains("\"minFlushId\": 1"));
        assertTrue(content.contains("\"maxFlushId\": 1"));
        assertTrue(content.contains("\"sstFile\": \"sst-000001-000001.sst\""));
        assertTrue(content.contains("\"state\": \"NEW\""));
        assertTrue(content.contains("\"minSequenceId\": 1"));
        assertTrue(content.contains("\"oldestWriteAtMillis\": " + meta.oldestWriteAtMillis()));
        assertTrue(content.contains("\"maxSequenceId\": 1"));
        assertTrue(content.contains("\"metaCrc32\""));
    }

    @Test
    void initReloadsExistingSstMetadata() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.persistFlushedSequenceId(meta.maxSequenceId());

        FileLocalStorageManager reloaded = storage();

        assertEquals(1, reloaded.metas().size());
        assertEquals(1L, reloaded.lastFlushedSequenceId());
        SSTMeta loaded = reloaded.metas().get(0);
        assertEquals(meta.runId(), loaded.runId());
        assertEquals(meta.entryCount(), loaded.entryCount());
        assertArrayEquals("v1".getBytes(), get(reloaded, loaded, "k1").orElseThrow().bytes());
    }

    @Test
    void corruptFooterCrcIsSkippedOnReload() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        try (RandomAccessFile file = new RandomAccessFile(meta.path().toFile(), "rw")) {
            file.seek(0);
            file.writeByte('X');
        }

        FileLocalStorageManager reloaded = new FileLocalStorageManager(
            new StorageConfig(tempDir.toString(), 0, 0, 0, 0)
        );
        reloaded.init();

        assertTrue(reloaded.metas().isEmpty());
    }

    @Test
    void pointLookupUsesCachedReaderAfterRegistration() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        assertArrayEquals("v1".getBytes(), get(storage, meta, "k1").orElseThrow().bytes());
        try (RandomAccessFile file = new RandomAccessFile(meta.path().toFile(), "rw")) {
            file.seek(file.length() - 1);
            file.writeByte('X');
        }

        assertArrayEquals("v1".getBytes(), get(storage, meta, "k1").orElseThrow().bytes());
    }

    @Test
    void missingReaderFailsWithExpectedSstDetailsInsteadOfReopening() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.deleteSST(meta);

        try (SSTReadSnapshot snapshot = storage.readSnapshot(List.of(meta))) {
            IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> snapshot.get(meta, new Key("k1".getBytes()))
            );

            assertTrue(error.getMessage().contains("Expected SST reader is missing"));
            assertTrue(error.getMessage().contains("runId=" + meta.runId()));
            assertTrue(error.getMessage().contains(meta.path().toString()));
        }
    }

    @Test
    void markSinkedPublishesTheBatchWithoutReplacingCachedReaders() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("k2", "v2".getBytes(), 2L));

        List<SSTMeta> sinked = storage.markSinked(List.of(first, second));

        assertEquals(List.of(first.runId(), second.runId()),
            sinked.stream().map(SSTMeta::runId).toList());
        assertTrue(sinked.stream().allMatch(meta -> meta.state() == SSTState.SINKED));
        assertArrayEquals("v1".getBytes(), get(storage, sinked.get(0), "k1").orElseThrow().bytes());
        assertArrayEquals("v2".getBytes(), get(storage, sinked.get(1), "k2").orElseThrow().bytes());
    }

    @Test
    void initIgnoresSstBeyondFlushBoundaryAsOrphan() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta orphan = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        FileLocalStorageManager reloaded = storage();

        assertTrue(Files.exists(orphan.path()));
        assertTrue(reloaded.metas().isEmpty());
        SSTMeta next = reloaded.flushToSST(immutable("k2", "v2".getBytes(), 2L));
        assertEquals(orphan.runId() + 1, next.runId());
    }

    @Test
    void corruptSstFailsReloadAfterFlushBoundaryWasPersisted() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.persistFlushedSequenceId(meta.maxSequenceId());

        try (RandomAccessFile file = new RandomAccessFile(meta.path().toFile(), "rw")) {
            file.seek(0);
            file.writeByte('X');
        }

        FileLocalStorageManager reloaded = new FileLocalStorageManager(
            new StorageConfig(tempDir.toString(), 0, 0, 0, 0)
        );

        assertThrows(IOException.class, reloaded::init);
    }

    @Test
    void corruptSstMetadataFailsReloadAfterFlushBoundaryWasPersisted() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.persistFlushedSequenceId(meta.maxSequenceId());

        Path metaPath = tempDir.resolve("sst-000001-000001.meta.json");
        String content = Files.readString(metaPath).replace("\"maxSequenceId\": 1", "\"maxSequenceId\": 2");
        Files.writeString(metaPath, content);

        FileLocalStorageManager reloaded = new FileLocalStorageManager(
            new StorageConfig(tempDir.toString(), 0, 0, 0, 0)
        );

        assertThrows(IOException.class, reloaded::init);
    }

    @Test
    void missingSstFailsReloadAfterFlushBoundaryWasPersisted() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.persistFlushedSequenceId(meta.maxSequenceId());
        Files.delete(meta.path());

        FileLocalStorageManager reloaded = new FileLocalStorageManager(
            new StorageConfig(tempDir.toString(), 0, 0, 0, 0)
        );

        IOException error = assertThrows(IOException.class, reloaded::init);
        assertTrue(error.getMessage().contains("SST files are missing after flush boundary was persisted"));
    }

    @Test
    void flushBoundaryIsMonotonic() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 10L));

        storage.persistFlushedSequenceId(meta.maxSequenceId());
        storage.persistFlushedSequenceId(8);

        FileLocalStorageManager reloaded = storage();
        assertEquals(10L, reloaded.lastFlushedSequenceId());
    }

    @Test
    void flushBoundaryPersistenceDoesNotUseTheVisibleSstViewMonitor() throws Exception {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        CountDownLatch monitorHeld = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> holder = executor.submit(() -> {
            synchronized (storage) {
                monitorHeld.countDown();
                try {
                    releaseMonitor.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("storage monitor holder interrupted", e);
                }
            }
        });
        try {
            assertTrue(monitorHeld.await(5, TimeUnit.SECONDS));

            Future<?> persisted = executor.submit(
                () -> storage.persistFlushedSequenceId(meta.maxSequenceId())
            );
            persisted.get(2, TimeUnit.SECONDS);
            assertEquals(meta.maxSequenceId(), storage.lastFlushedSequenceId());
        } finally {
            releaseMonitor.countDown();
            holder.get(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }

    @Test
    void compactSSTsMergesContinuousFlushRangesAndKeepsLatestSequence() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable(
            "a", "old-a".getBytes(), 1L,
            "b", "old-b".getBytes(), 2L
        ));
        SSTMeta second = storage.flushToSST(immutable(
            "a", "new-a".getBytes(), 3L,
            "c", null, 4L
        ));
        long oldestWriteAtMillis = Math.min(first.oldestWriteAtMillis(), second.oldestWriteAtMillis());

        SSTMeta compacted = storage.compactSSTs(List.of(first, second));

        assertEquals(1L, compacted.minFlushId());
        assertEquals(2L, compacted.maxFlushId());
        assertEquals(3L, compacted.entryCount());
        assertEquals(oldestWriteAtMillis, compacted.oldestWriteAtMillis());
        assertTrue(Files.exists(tempDir.resolve("sst-000001-000002.sst")));
        assertFalse(Files.exists(first.path()));
        assertFalse(Files.exists(second.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
        assertFalse(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
        assertArrayEquals("new-a".getBytes(), get(storage, compacted, "a").orElseThrow().bytes());
        assertArrayEquals("old-b".getBytes(), get(storage, compacted, "b").orElseThrow().bytes());
        assertTrue(get(storage, compacted, "c").orElseThrow().isTombstone());

        storage.persistFlushedSequenceId(compacted.maxSequenceId());
        FileLocalStorageManager reloaded = storage();
        assertEquals(List.of(compacted.runId()), reloaded.metas().stream().map(SSTMeta::runId).toList());
        assertEquals(oldestWriteAtMillis, reloaded.metas().get(0).oldestWriteAtMillis());
    }

    @Test
    void visibleSnapshotAtomicallyPinsItsSstViewUntilClose() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("a", "old-a".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("b", "old-b".getBytes(), 2L));

        try (SSTReadSnapshot leases = storage.readVisibleSnapshot()) {
            assertEquals(List.of(first.runId(), second.runId()),
                leases.metas().stream().map(SSTMeta::runId).toList());
            storage.compactSSTs(List.of(first, second));

            assertTrue(Files.exists(first.path()));
            assertTrue(Files.exists(second.path()));
            assertTrue(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
            assertTrue(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
            SSTEntryIterator iterator = leases.openIterator(first);
            assertTrue(iterator.hasNext());
            assertEquals("a", new String(iterator.next().key().bytes()));
        }

        assertFalse(Files.exists(first.path()));
        assertFalse(Files.exists(second.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
        assertFalse(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
    }

    @Test
    void initPrefersCompactedMetaWhenInputMetasRemainAfterCrash() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("a", "old-a".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("a", "new-a".getBytes(), 2L));
        byte[] firstData = Files.readAllBytes(first.path());
        byte[] secondData = Files.readAllBytes(second.path());
        storage.persistFlushedSequenceId(second.maxSequenceId());

        SSTMeta compacted = storage.compactSSTs(List.of(first, second));
        SSTMetaStore metaStore = new SSTMetaStore(tempDir);
        metaStore.init();
        Files.write(first.path(), firstData);
        Files.write(second.path(), secondData);
        metaStore.save(first);
        metaStore.save(second);

        FileLocalStorageManager reloaded = storage();

        assertEquals(1, reloaded.metas().size());
        assertEquals(compacted.runId(), reloaded.metas().get(0).runId());
        assertEquals(1L, reloaded.metas().get(0).minFlushId());
        assertEquals(2L, reloaded.metas().get(0).maxFlushId());
    }

    @Test
    void compactRejectsNonContinuousFlushRanges() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("a", "a".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("b", "b".getBytes(), 2L));
        SSTMeta third = storage.flushToSST(immutable("c", "c".getBytes(), 3L));

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> storage.compactSSTs(List.of(first, third))
        );

        assertTrue(error.getMessage().contains("continuous"));
        assertTrue(Files.exists(second.path()));
    }

    private static ImmutableMemTable blockIterator(
            ImmutableMemTable delegate,
            CountDownLatch entered,
            CountDownLatch release) {
        return new ImmutableMemTable() {
            @Override
            public Value get(Key key) {
                return delegate.get(key);
            }

            @Override
            public Iterator<org.qwh.pms.core.memtable.model.Entry> iterator() {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("blocked immutable iterator interrupted", e);
                }
                return delegate.iterator();
            }

            @Override
            public Iterator<org.qwh.pms.core.memtable.model.Entry> iterator(
                    Key startInclusive,
                    Optional<Key> endExclusive) {
                return delegate.iterator(startInclusive, endExclusive);
            }

            @Override
            public long estimatedSize() {
                return delegate.estimatedSize();
            }

            @Override
            public int estimatedEntryCount() {
                return delegate.estimatedEntryCount();
            }

            @Override
            public long minSequenceId() {
                return delegate.minSequenceId();
            }

            @Override
            public long maxSequenceId() {
                return delegate.maxSequenceId();
            }

            @Override
            public long oldestWriteAtMillis() {
                return delegate.oldestWriteAtMillis();
            }
        };
    }
}
