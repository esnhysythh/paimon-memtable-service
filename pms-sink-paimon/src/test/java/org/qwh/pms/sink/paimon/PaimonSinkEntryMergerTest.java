package org.qwh.pms.sink.paimon;

import org.junit.jupiter.api.Test;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.storage.SSTEntryIterator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSinkEntryMergerTest {

    private final PaimonSinkEntryMerger merger = new PaimonSinkEntryMerger();

    @Test
    void mergesInputsWithoutDuplicateKeys() {
        List<Entry> merged = drain(merger.mergeLatest(List.of(
            iterator(entry("a", "old-a", 1), entry("c", "old-c", 1)),
            iterator(entry("b", "new-b", 2), entry("d", "new-d", 2))
        )));

        assertEquals(List.of("a", "b", "c", "d"), keys(merged));
        assertEquals(List.of(1L, 2L, 1L, 2L), sequences(merged));
    }

    @Test
    void keepsLargestSequenceForDuplicateKeys() {
        List<Entry> merged = drain(merger.mergeLatest(List.of(
            iterator(entry("a", "old-a", 1), entry("c", "old-c", 1)),
            iterator(entry("a", "new-a", 3), entry("b", "new-b", 2))
        )));

        assertEquals(List.of("a", "b", "c"), keys(merged));
        assertEquals(List.of("new-a", "new-b", "old-c"), values(merged));
        assertEquals(List.of(3L, 2L, 1L), sequences(merged));
    }

    @Test
    void preservesNewestTombstone() {
        List<Entry> merged = drain(merger.mergeLatest(List.of(
            iterator(entry("a", "old-a", 1)),
            iterator(tombstone("a", 5))
        )));

        assertEquals(List.of("a"), keys(merged));
        assertTrue(merged.get(0).value().isTombstone());
        assertEquals(5L, merged.get(0).value().sequenceId());
    }

    @Test
    void collapsesConsecutiveDuplicateKeysFromSameInput() {
        List<Entry> merged = drain(merger.mergeLatest(List.of(
            iterator(entry("a", "old-a", 1), entry("a", "new-a", 4), entry("b", "b", 2))
        )));

        assertEquals(List.of("a", "b"), keys(merged));
        assertEquals(List.of("new-a", "b"), values(merged));
    }

    @Test
    void closesInputsWhenExhausted() {
        TrackingIterator input = iterator(entry("a", "a", 1));

        drain(merger.mergeLatest(List.of(input)));

        assertTrue(input.closed);
    }

    @Test
    void closesInputsWhenOutputIsClosedEarly() {
        TrackingIterator input = iterator(entry("a", "a", 1), entry("b", "b", 2));
        SSTEntryIterator output = merger.mergeLatest(List.of(input));

        assertTrue(output.hasNext());
        output.close();

        assertTrue(input.closed);
    }

    private static List<Entry> drain(SSTEntryIterator iterator) {
        List<Entry> entries = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }

    private static TrackingIterator iterator(Entry... entries) {
        return new TrackingIterator(List.of(entries));
    }

    private static Entry entry(String key, String value, long sequenceId) {
        return new Entry(key(key), new Value(value.getBytes(StandardCharsets.UTF_8), sequenceId));
    }

    private static Entry tombstone(String key, long sequenceId) {
        return new Entry(key(key), Value.tombstone(sequenceId));
    }

    private static Key key(String key) {
        return new Key(key.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> keys(List<Entry> entries) {
        return entries.stream().map(entry -> string(entry.key().bytes())).toList();
    }

    private static List<String> values(List<Entry> entries) {
        return entries.stream().map(entry -> string(entry.value().bytes())).toList();
    }

    private static List<Long> sequences(List<Entry> entries) {
        return entries.stream().map(entry -> entry.value().sequenceId()).toList();
    }

    private static String string(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class TrackingIterator implements SSTEntryIterator {
        private final List<Entry> entries;
        private int index;
        private boolean closed;

        private TrackingIterator(List<Entry> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return index < entries.size();
        }

        @Override
        public Entry next() {
            return entries.get(index++);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
