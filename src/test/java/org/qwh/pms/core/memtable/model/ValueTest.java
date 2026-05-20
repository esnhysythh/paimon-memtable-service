package org.qwh.pms.core.memtable.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ValueTest {

    @Test
    void isTombstoneForNull() {
        assertTrue(Value.TOMBSTONE.isTombstone());
    }

    @Test
    void isNotTombstoneForNonNull() {
        assertFalse(new Value("data".getBytes()).isTombstone());
    }

    @Test
    void sizeReturnsZeroForTombstone() {
        assertEquals(0, Value.TOMBSTONE.size());
    }

    @Test
    void sizeReturnsByteLengthForValue() {
        assertEquals(3, new Value("abc".getBytes()).size());
    }

    // ── equals / hashCode ──

    @Test
    void equalsReturnsTrueForSameContent() {
        Value v1 = new Value("abc".getBytes());
        Value v2 = new Value("abc".getBytes());
        assertEquals(v1, v2, "Values with same byte content should be equal");
    }

    @Test
    void sequenceIdIsMetadataNotValueIdentity() {
        Value v1 = new Value("abc".getBytes(), 1);
        Value v2 = new Value("abc".getBytes(), 2);
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertEquals(1L, v1.sequenceId());
        assertEquals(2L, v2.sequenceId());
    }

    @Test
    void equalsReturnsFalseForDifferentContent() {
        Value v1 = new Value("abc".getBytes());
        Value v2 = new Value("abd".getBytes());
        assertNotEquals(v1, v2);
    }

    @Test
    void equalsReturnsTrueForTwoTombstones() {
        assertEquals(Value.TOMBSTONE, new Value(null));
    }

    @Test
    void equalsReturnsFalseForTombstoneVsData() {
        assertNotEquals(Value.TOMBSTONE, new Value("data".getBytes()));
        assertNotEquals(new Value("data".getBytes()), Value.TOMBSTONE);
    }

    @Test
    void equalsIsReflexiveAndSymmetric() {
        Value v1 = new Value("test".getBytes());
        Value v2 = new Value("test".getBytes());
        assertEquals(v1, v1, "equals should be reflexive");
        assertEquals(v1.equals(v2), v2.equals(v1), "equals should be symmetric");
    }

    @Test
    void hashCodeConsistentWithEquals() {
        Value v1 = new Value("abc".getBytes());
        Value v2 = new Value("abc".getBytes());
        assertEquals(v1.hashCode(), v2.hashCode(), "Equal values must have same hashCode");
    }

    @Test
    void tombstoneHashCodeIsZero() {
        assertEquals(0, Value.TOMBSTONE.hashCode());
    }

    @Test
    void worksInHashSet() {
        Set<Value> set = new HashSet<>();
        set.add(new Value("v1".getBytes()));
        set.add(new Value("v1".getBytes()));
        set.add(new Value("v2".getBytes()));
        set.add(Value.TOMBSTONE);
        set.add(new Value(null));
        assertEquals(3, set.size(), "HashSet should deduplicate equal values (v1, v2, tombstone)");
    }

    @Test
    void worksInHashMap() {
        HashMap<Value, String> map = new HashMap<>();
        map.put(new Value("k".getBytes()), "v1");
        map.put(new Value("k".getBytes()), "v2");
        assertEquals(1, map.size());
        assertEquals("v2", map.get(new Value("k".getBytes())));
    }

    @Test
    void equalsWithBinaryContent() {
        Value v1 = new Value(new byte[]{0x00, (byte) 0x80, (byte) 0xFF});
        Value v2 = new Value(new byte[]{0x00, (byte) 0x80, (byte) 0xFF});
        assertEquals(v1, v2, "Binary values with same content should be equal");
        assertEquals(v1.hashCode(), v2.hashCode());
    }
}
