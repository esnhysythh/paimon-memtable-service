package org.qwh.pms.core.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListImmutableMemTableTest {

    private ImmutableMemTable immutable;

    private static final MemTableConfig TEST_CONFIG = new MemTableConfig(1_000_000, 256);

    @BeforeEach
    void setUp() {
        SkipListCurMemTable cur = new SkipListCurMemTable(TEST_CONFIG);
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
    void estimatedEntryCount() {
        assertEquals(3, immutable.estimatedEntryCount());
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

    @Test
    void refCountDecrementBelowZeroGoesNegative() {
        // AtomicLong allows decrement below zero — this is by design.
        // The caller (BucketDirector) is responsible for checking refCount before evict.
        assertEquals(0, immutable.refCount());
        immutable.decrementRef();
        assertEquals(-1, immutable.refCount());
    }

    @Test
    void refCountConcurrentIncrementDecrement() throws Exception {
        int threadCount = 8;
        int opsPerThread = 10_000;
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            threads.add(new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    if (tid % 2 == 0) {
                        immutable.incrementRef();
                    } else {
                        immutable.decrementRef();
                    }
                }
            }));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // 4 threads increment, 4 threads decrement — net should be 0
        assertEquals(0, immutable.refCount());
    }
}
