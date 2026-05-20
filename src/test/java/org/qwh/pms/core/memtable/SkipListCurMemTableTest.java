package org.qwh.pms.core.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.*;

class SkipListCurMemTableTest {

    private SkipListCurMemTable table;

    private static final MemTableConfig TEST_CONFIG = new MemTableConfig(1_000_000, 256);

    @BeforeEach
    void setUp() {
        table = new SkipListCurMemTable(TEST_CONFIG);
    }

    private Key key(String s) {
        return new Key(s.getBytes());
    }

    private Value value(String s) {
        return new Value(s.getBytes());
    }

    // ── put / get ──

    @Test
    void putAndGet() {
        table.put(key("k1"), value("v1"));
        Value result = table.get(key("k1"));
        assertNotNull(result);
        assertArrayEquals("v1".getBytes(), result.bytes());
    }

    @Test
    void getMissingKeyReturnsNull() {
        assertNull(table.get(key("nonexistent")));
    }

    @Test
    void putOverwritesExisting() {
        table.put(key("k1"), value("v1"));
        table.put(key("k1"), value("v2"));
        assertArrayEquals("v2".getBytes(), table.get(key("k1")).bytes());
    }

    // ── delete ──

    @Test
    void deleteSetsTombstone() {
        table.put(key("k1"), value("v1"));
        table.delete(key("k1"));
        Value result = table.get(key("k1"));
        assertNotNull(result);
        assertTrue(result.isTombstone());
    }

    @Test
    void deleteOnNonExistentKeyCreatesTombstone() {
        table.delete(key("k1"));
        Value result = table.get(key("k1"));
        assertNotNull(result);
        assertTrue(result.isTombstone());
    }

    @Test
    void putAfterDeleteOverwrites() {
        table.put(key("k1"), value("v1"));
        table.delete(key("k1"));
        table.put(key("k1"), value("v2"));
        Value result = table.get(key("k1"));
        assertFalse(result.isTombstone());
        assertArrayEquals("v2".getBytes(), result.bytes());
    }

    // ── entryCount / estimatedSize ──

    @Test
    void entryCountTracksInsertions() {
        assertEquals(0, table.estimatedEntryCount());
        table.put(key("k1"), value("v1"));
        assertEquals(1, table.estimatedEntryCount());
        table.put(key("k2"), value("v2"));
        assertEquals(2, table.estimatedEntryCount());
    }

    @Test
    void entryCountUnchangedOnOverwrite() {
        table.put(key("k1"), value("v1"));
        assertEquals(1, table.estimatedEntryCount());
        table.put(key("k1"), value("v2"));
        assertEquals(1, table.estimatedEntryCount());
    }

    @Test
    void estimatedSizeGrowsWithPuts() {
        long size0 = table.estimatedSize();
        table.put(key("k1"), value("v1"));
        long size1 = table.estimatedSize();
        assertTrue(size1 > size0);
    }

    // ── freeze ──

    @Test
    void freezeReturnsImmutableAndResetsCurrent() {
        table.put(key("k1"), value("v1"));
        table.put(key("k2"), value("v2"));

        ImmutableMemTable frozen = table.freeze();

        // Frozen should contain the data
        assertNotNull(frozen.get(key("k1")));
        assertNotNull(frozen.get(key("k2")));
        assertEquals(2, frozen.estimatedEntryCount());

        // Current should be empty
        assertEquals(0, table.estimatedEntryCount());
        assertNull(table.get(key("k1")));
    }

    @Test
    void freezeThenWriteToCurrentDoesNotAffectFrozen() {
        table.put(key("k1"), value("v1"));
        ImmutableMemTable frozen = table.freeze();

        table.put(key("k1"), value("v2"));
        // Frozen should still have v1
        assertArrayEquals("v1".getBytes(), frozen.get(key("k1")).bytes());
    }

    @Test
    void multipleFreezesProduceIndependentTables() {
        table.put(key("k1"), value("v1"));
        ImmutableMemTable frozen1 = table.freeze();

        table.put(key("k2"), value("v2"));
        ImmutableMemTable frozen2 = table.freeze();

        assertEquals(1, frozen1.estimatedEntryCount());
        assertEquals(1, frozen2.estimatedEntryCount());
        assertNotNull(frozen1.get(key("k1")));
        assertNull(frozen1.get(key("k2")));
        assertNull(frozen2.get(key("k1")));
        assertNotNull(frozen2.get(key("k2")));
    }

    // ── iterator ──

    @Test
    void iteratorReturnsAllEntries() {
        table.put(key("k1"), value("v1"));
        table.put(key("k2"), value("v2"));
        table.put(key("k3"), value("v3"));

        List<Entry> entries = new ArrayList<>();
        table.iterator().forEachRemaining(entries::add);

        assertEquals(3, entries.size());
    }

    @Test
    void iteratorReturnsEntriesInKeyOrder() {
        table.put(key("k3"), value("v3"));
        table.put(key("k1"), value("v1"));
        table.put(key("k2"), value("v2"));

        List<Entry> entries = new ArrayList<>();
        table.iterator().forEachRemaining(entries::add);

        assertEquals("k1", new String(entries.get(0).key().bytes()));
        assertEquals("k2", new String(entries.get(1).key().bytes()));
        assertEquals("k3", new String(entries.get(2).key().bytes()));
    }

    @Test
    void emptyTableIteratorHasNoElements() {
        assertFalse(table.iterator().hasNext());
    }

    // ── shouldFreeze ──

    @Test
    void shouldFreezeReturnsFalseBelowThreshold() {
        assertFalse(table.shouldFreeze());
    }

    @Test
    void shouldFreezeReturnsTrueAtEntryThreshold() {
        SkipListCurMemTable smallTable = new SkipListCurMemTable(new MemTableConfig(3, 256));
        smallTable.put(key("k1"), value("v1"));
        smallTable.put(key("k2"), value("v2"));
        assertFalse(smallTable.shouldFreeze());
        smallTable.put(key("k3"), value("v3"));
        assertTrue(smallTable.shouldFreeze());
    }

    // ── concurrency ──

    @Test
    void concurrentWrites() throws Exception {
        int threadCount = 8;
        int opsPerThread = 10_000;
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            threads.add(new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    table.put(new Key(("t" + tid + "-" + i).getBytes()), new Value(("v" + tid + "-" + i).getBytes()));
                }
            }));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            for (int i = 0; i < opsPerThread; i++) {
                Value val = table.get(new Key(("t" + tid + "-" + i).getBytes()));
                assertNotNull(val, "Missing entry for thread " + tid + " op " + i);
                assertArrayEquals(("v" + tid + "-" + i).getBytes(), val.bytes());
            }
        }
    }

    @Test
    void concurrentMixedReadWrite() throws Exception {
        // Pre-populate with stable data that readers will verify
        int preCount = 1000;
        for (int i = 0; i < preCount; i++) {
            table.put(new Key(("key-" + i).getBytes()), new Value(("val-" + i).getBytes()));
        }

        int writerCount = 4;
        int readerCount = 4;
        int opsPerThread = 5000;
        CyclicBarrier barrier = new CyclicBarrier(writerCount + readerCount);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        List<Thread> threads = new ArrayList<>();

        // Writers: write unique keys that don't overlap with pre-populated data
        for (int t = 0; t < writerCount; t++) {
            final int tid = t;
            threads.add(new Thread(() -> {
                try { barrier.await(); } catch (Exception e) { return; }
                for (int i = 0; i < opsPerThread; i++) {
                    table.put(new Key(("w-" + tid + "-" + i).getBytes()),
                              new Value(("wv-" + tid + "-" + i).getBytes()));
                }
            }));
        }

        // Readers: verify pre-populated keys — they must never return wrong values
        for (int t = 0; t < readerCount; t++) {
            threads.add(new Thread(() -> {
                try { barrier.await(); } catch (Exception e) { return; }
                for (int i = 0; i < opsPerThread; i++) {
                    int idx = i % preCount;
                    try {
                        Value val = table.get(new Key(("key-" + idx).getBytes()));
                        assertNotNull(val, "Pre-populated key-" + idx + " should always be found");
                        assertFalse(val.isTombstone(), "key-" + idx + " should not be a tombstone");
                        assertArrayEquals(("val-" + idx).getBytes(), val.bytes(),
                            "key-" + idx + " returned wrong value under concurrent writes");
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            }));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        assertTrue(errors.isEmpty(), "Reader errors during concurrent mixed read/write: " + errors);
    }
}
