package org.qwh.pms.core.memtable;

import org.junit.jupiter.api.Test;
import org.qwh.pms.core.memtable.model.Key;

import static org.junit.jupiter.api.Assertions.*;

class KeyTest {

    @Test
    void nullBytesRejected() {
        assertThrows(NullPointerException.class, () -> new Key(null));
    }

    @Test
    void compareToIsUnsignedByteOrder() {
        // 0xFF = 255 unsigned, 0x00 = 0
        // In unsigned comparison, 0xFF > 0x00
        // In signed Java byte comparison, (byte)0xFF = -1 < 0
        Key high = new Key(new byte[]{(byte) 0xFF});
        Key low = new Key(new byte[]{0x00});
        assertTrue(high.compareTo(low) > 0, "0xFF should sort after 0x00 (unsigned)");
        assertTrue(low.compareTo(high) < 0, "0x00 should sort before 0xFF (unsigned)");
    }

    @Test
    void compareToPrefixOrdering() {
        Key prefix = new Key("abc".getBytes());
        Key longer = new Key("abcd".getBytes());
        assertTrue(prefix.compareTo(longer) < 0, "shorter key sorts before longer key with same prefix");
    }

    @Test
    void compareToEqualKeys() {
        Key k1 = new Key("same".getBytes());
        Key k2 = new Key("same".getBytes());
        assertEquals(0, k1.compareTo(k2));
    }

    @Test
    void sizeReturnsByteLength() {
        Key k = new Key("hello".getBytes());
        assertEquals(5, k.size());
    }

    @Test
    void unsignedByteComparisonAcrossBoundary() {
        // Keys that mix bytes below and above 0x80
        // In Paimon primary key order, all bytes are unsigned
        Key a = new Key(new byte[]{0x7F});           // 127 unsigned
        Key b = new Key(new byte[]{(byte) 0x80});    // 128 unsigned
        Key c = new Key(new byte[]{(byte) 0xFF});    // 255 unsigned

        assertTrue(a.compareTo(b) < 0, "0x7F < 0x80 in unsigned order");
        assertTrue(b.compareTo(c) < 0, "0x80 < 0xFF in unsigned order");
    }
}
