package org.qwh.pms.testkit.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

public final class DeterministicDataGenerator {
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private DeterministicDataGenerator() {}

    public static TestDataSet standardScenario(int count, int payloadSize, long seed) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive: " + count);
        }
        if (payloadSize < 0) {
            throw new IllegalArgumentException("payloadSize must not be negative: " + payloadSize);
        }

        List<TestRecord> inserts = new ArrayList<>(count);
        List<TestRecord> updates = new ArrayList<>();
        List<Long> deletes = new ArrayList<>();
        Map<Long, TestRecord> expected = new LinkedHashMap<>();
        for (long id = 0; id < count; id++) {
            TestRecord record = record(id, 0, payloadSize, seed);
            inserts.add(record);
            expected.put(id, record);
        }
        for (long id = 0; id < count; id += 5) {
            TestRecord updated = record(id, 1, payloadSize, seed);
            updates.add(updated);
            expected.put(id, updated);
        }
        for (long id = 3; id < count; id += 11) {
            deletes.add(id);
            expected.remove(id);
        }
        return new TestDataSet(seed, inserts, updates, deletes, expected);
    }

    public static TestRecord record(long id, int version, int payloadSize, long seed) {
        if (payloadSize < 0) {
            throw new IllegalArgumentException("payloadSize must not be negative: " + payloadSize);
        }
        SplittableRandom random = new SplittableRandom(mix(seed, id, version));
        StringBuilder payload = new StringBuilder(payloadSize);
        for (int i = 0; i < payloadSize; i++) {
            payload.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        }
        return new TestRecord(id, payload.toString(), version);
    }

    private static long mix(long seed, long id, int version) {
        long value = seed ^ Long.rotateLeft(id * 0x9E3779B97F4A7C15L, 17);
        return value ^ ((long) version * 0xD1B54A32D192ED03L);
    }
}
