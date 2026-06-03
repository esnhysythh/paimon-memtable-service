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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
            cur.put(new Key(key.getBytes()), value == null ? Value.tombstone(sequenceId) : new Value(value, sequenceId));
        }
        return cur.freeze();
    }

    @Test
    void flushAndReadPut() throws IOException {
        FileLocalStorageManager storage = storage();
        ImmutableMemTable memTable = immutable(
            "k1", "v1".getBytes(), 1L,
            "k2", "v2".getBytes(), 2L
        );

        SSTMeta meta = storage.flushToSST(memTable);
        Optional<Value> value = storage.get(meta, new Key("k2".getBytes()));

        assertTrue(value.isPresent());
        assertFalse(value.get().isTombstone());
        assertArrayEquals("v2".getBytes(), value.get().bytes());
        assertEquals(2L, value.get().sequenceId());
    }

    @Test
    void missReturnsEmpty() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));

        assertTrue(storage.get(meta, new Key("missing".getBytes())).isEmpty());
    }

    @Test
    void tombstoneReturnsValueWithNullBytes() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", null, 7L));

        Optional<Value> value = storage.get(meta, new Key("k1".getBytes()));

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
        try (SSTEntryIterator iterator = storage.openIterator(meta)) {
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
        try (SSTEntryIterator iterator = storage.openIterator(meta)) {
            storage.deleteSST(meta);

            assertTrue(storage.metas().isEmpty());
            assertTrue(Files.exists(meta.path()));
            while (iterator.hasNext()) {
                keys.add(new String(iterator.next().key().bytes()));
            }
        }

        assertEquals(List.of("k1", "k2"), keys);
        assertFalse(Files.exists(meta.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
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
        try (SSTEntryIterator iterator = storage.openIterator(
            meta,
            new Key("b".getBytes()),
            Optional.of(new Key("d".getBytes()))
        )) {
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
        assertTrue(content.contains("\"runId\": " + meta.runId()));
        assertTrue(content.contains("\"minFlushId\": 1"));
        assertTrue(content.contains("\"maxFlushId\": 1"));
        assertTrue(content.contains("\"sstFile\": \"sst-000001-000001.new.sst\""));
        assertTrue(content.contains("\"state\": \"NEW\""));
        assertTrue(content.contains("\"minSequenceId\": 1"));
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
        assertArrayEquals("v1".getBytes(), reloaded.get(loaded, new Key("k1".getBytes())).orElseThrow().bytes());
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

        assertArrayEquals("v1".getBytes(), storage.get(meta, new Key("k1".getBytes())).orElseThrow().bytes());
        try (RandomAccessFile file = new RandomAccessFile(meta.path().toFile(), "rw")) {
            file.seek(file.length() - 1);
            file.writeByte('X');
        }

        assertArrayEquals("v1".getBytes(), storage.get(meta, new Key("k1".getBytes())).orElseThrow().bytes());
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

        SSTMeta compacted = storage.compactSSTs(List.of(first, second));

        assertEquals(1L, compacted.minFlushId());
        assertEquals(2L, compacted.maxFlushId());
        assertEquals(3L, compacted.entryCount());
        assertTrue(Files.exists(tempDir.resolve("sst-000001-000002.new.sst")));
        assertFalse(Files.exists(first.path()));
        assertFalse(Files.exists(second.path()));
        assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
        assertFalse(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
        assertArrayEquals("new-a".getBytes(), storage.get(compacted, new Key("a".getBytes())).orElseThrow().bytes());
        assertArrayEquals("old-b".getBytes(), storage.get(compacted, new Key("b".getBytes())).orElseThrow().bytes());
        assertTrue(storage.get(compacted, new Key("c".getBytes())).orElseThrow().isTombstone());

        storage.persistFlushedSequenceId(compacted.maxSequenceId());
        FileLocalStorageManager reloaded = storage();
        assertEquals(List.of(compacted.runId()), reloaded.metas().stream().map(SSTMeta::runId).toList());
    }

    @Test
    void compactDelaysInputSstDataFileDeletionUntilIteratorLeaseCloses() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta first = storage.flushToSST(immutable("a", "old-a".getBytes(), 1L));
        SSTMeta second = storage.flushToSST(immutable("b", "old-b".getBytes(), 2L));

        try (SSTEntryIterator iterator = storage.openIterator(first)) {
            storage.compactSSTs(List.of(first, second));

            assertTrue(Files.exists(first.path()));
            assertFalse(Files.exists(second.path()));
            assertFalse(Files.exists(tempDir.resolve("sst-000001-000001.meta.json")));
            assertFalse(Files.exists(tempDir.resolve("sst-000002-000002.meta.json")));
            assertTrue(iterator.hasNext());
            assertEquals("a", new String(iterator.next().key().bytes()));
        }

        assertFalse(Files.exists(first.path()));
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
}
