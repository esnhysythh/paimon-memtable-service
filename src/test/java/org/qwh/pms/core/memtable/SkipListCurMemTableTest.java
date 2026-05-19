package org.qwh.pms.core.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListCurMemTableTest {

    private SkipListCurMemTable table;

    @BeforeEach
    void setUp() {
        table = new SkipListCurMemTable();
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
        assertEquals(0, table.entryCount());
        table.put(key("k1"), value("v1"));
        assertEquals(1, table.entryCount());
        table.put(key("k2"), value("v2"));
        assertEquals(2, table.entryCount());
    }

    @Test
    void entryCountUnchangedOnOverwrite() {
        table.put(key("k1"), value("v1"));
        assertEquals(1, table.entryCount());
        table.put(key("k1"), value("v2"));
        assertEquals(1, table.entryCount());
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
        assertEquals(2, frozen.entryCount());

        // Current should be empty
        assertEquals(0, table.entryCount());
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

        assertEquals(1, frozen1.entryCount());
        assertEquals(1, frozen2.entryCount());
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
}
