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
    void initReloadsExistingSstMetadata() throws IOException {
        FileLocalStorageManager storage = storage();
        SSTMeta meta = storage.flushToSST(immutable("k1", "v1".getBytes(), 1L));
        storage.persistFlushedSequenceId(meta.maxSequenceId());

        FileLocalStorageManager reloaded = storage();

        assertEquals(1, reloaded.metas().size());
        assertEquals(1L, reloaded.lastFlushedSequenceId());
        SSTMeta loaded = reloaded.metas().get(0);
        assertEquals(meta.fileId(), loaded.fileId());
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
    void flushBoundaryIsMonotonic() throws IOException {
        FileLocalStorageManager storage = storage();

        storage.persistFlushedSequenceId(10);
        storage.persistFlushedSequenceId(8);

        FileLocalStorageManager reloaded = storage();
        assertEquals(10L, reloaded.lastFlushedSequenceId());
    }
}
