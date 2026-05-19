package org.qwh.pms.core.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListImmutableMemTableTest {

    private ImmutableMemTable immutable;

    @BeforeEach
    void setUp() {
        SkipListCurMemTable cur = new SkipListCurMemTable();
        cur.put(new Key("k1".getBytes()), new Value("v1".getBytes()));
        cur.put(new Key("k2".getBytes()), new Value("v2".getBytes()));
        cur.delete(new Key("k3".getBytes()));
        immutable = cur.freeze();
    }

    private Key key(String s) {
        return new Key(s.getBytes());
    }

    @Test
    void getReturnsValue() {
        Value v = immutable.get(key("k1"));
        assertNotNull(v);
        assertArrayEquals("v1".getBytes(), v.bytes());
    }

    @Test
    void getReturnsTombstoneForDeletedKey() {
        Value v = immutable.get(key("k3"));
        assertNotNull(v);
        assertTrue(v.isTombstone());
    }

    @Test
    void getReturnsNullForMissingKey() {
        assertNull(immutable.get(key("nonexistent")));
    }

    @Test
    void entryCount() {
        assertEquals(3, immutable.entryCount());
    }

    @Test
    void estimatedSize() {
        assertTrue(immutable.estimatedSize() > 0);
    }

    @Test
    void iteratorReturnsAllEntriesInOrder() {
        List<Entry> entries = new ArrayList<>();
        immutable.iterator().forEachRemaining(entries::add);

        assertEquals(3, entries.size());
        assertEquals("k1", new String(entries.get(0).key().bytes()));
        assertEquals("k2", new String(entries.get(1).key().bytes()));
        assertEquals("k3", new String(entries.get(2).key().bytes()));
        assertTrue(entries.get(2).isTombstone());
    }

    @Test
    void refCountOperations() {
        assertEquals(0, immutable.refCount());
        immutable.incrementRef();
        assertEquals(1, immutable.refCount());
        immutable.incrementRef();
        assertEquals(2, immutable.refCount());
        immutable.decrementRef();
        assertEquals(1, immutable.refCount());
    }
}
