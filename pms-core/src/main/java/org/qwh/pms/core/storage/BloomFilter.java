package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;

import java.io.ByteArrayOutputStream;
import java.util.BitSet;

final class BloomFilter {
    private final int bitCount;
    private final int hashCount;
    private final int keyCount;
    private final BitSet bits;

    private BloomFilter(int bitCount, int hashCount, int keyCount, BitSet bits) {
        this.bitCount = Math.max(1, bitCount);
        this.hashCount = Math.max(1, hashCount);
        this.keyCount = Math.max(0, keyCount);
        this.bits = bits;
    }

    static BloomFilter create(int expectedInsertions, double fpp) {
        int n = Math.max(1, expectedInsertions);
        double p = fpp <= 0 || fpp >= 1 ? SSTFormat.DEFAULT_BLOOM_FPP : fpp;
        int m = Math.max(64, (int) Math.ceil(-(n * Math.log(p)) / (Math.log(2) * Math.log(2))));
        int k = Math.max(1, (int) Math.round((m / (double) n) * Math.log(2)));
        return new BloomFilter(m, k, 0, new BitSet(m));
    }

    BloomFilter add(Key key) {
        long h1 = hash64(key.bytes(), 0x9E3779B97F4A7C15L);
        long h2 = hash64(key.bytes(), 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < hashCount; i++) {
            int bit = positiveModulo(h1 + i * h2, bitCount);
            bits.set(bit);
        }
        return new BloomFilter(bitCount, hashCount, keyCount + 1, bits);
    }

    boolean mightContain(Key key) {
        long h1 = hash64(key.bytes(), 0x9E3779B97F4A7C15L);
        long h2 = hash64(key.bytes(), 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < hashCount; i++) {
            int bit = positiveModulo(h1 + i * h2, bitCount);
            if (!bits.get(bit)) {
                return false;
            }
        }
        return true;
    }

    byte[] encode() {
        byte[] bitset = bits.toByteArray();
        ByteArrayOutputStream out = new ByteArrayOutputStream(12 + bitset.length);
        StorageCoding.writeIntLE(out, bitCount);
        StorageCoding.writeIntLE(out, hashCount);
        StorageCoding.writeIntLE(out, keyCount);
        out.writeBytes(bitset);
        return out.toByteArray();
    }

    static BloomFilter decode(byte[] data) {
        if (data.length < 12) {
            throw new IllegalArgumentException("BloomFilter block is too short");
        }
        int bitCount = StorageCoding.readIntLE(data, 0);
        int hashCount = StorageCoding.readIntLE(data, 4);
        int keyCount = StorageCoding.readIntLE(data, 8);
        byte[] bitset = new byte[data.length - 12];
        System.arraycopy(data, 12, bitset, 0, bitset.length);
        return new BloomFilter(bitCount, hashCount, keyCount, BitSet.valueOf(bitset));
    }

    private static int positiveModulo(long value, int modulus) {
        return (int) Long.remainderUnsigned(value, modulus);
    }

    private static long hash64(byte[] data, long seed) {
        long h = seed;
        for (byte b : data) {
            h ^= b & 0xFFL;
            h *= 0x100000001B3L;
            h ^= h >>> 32;
        }
        return h;
    }
}
