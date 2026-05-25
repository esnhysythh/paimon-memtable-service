package org.qwh.pms.sink.paimon;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.storage.SSTEntryIterator;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Streaming k-way merge for ordered PMS SST entries.
 *
 * <p>Inputs must be sorted by PMS key. When the same key appears in multiple inputs, the entry with
 * the largest value sequence id wins. The output remains ordered by key and preserves tombstones.
 */
public final class PaimonSinkEntryMerger {

    public SSTEntryIterator mergeLatest(List<? extends SSTEntryIterator> inputs) {
        return new MergedIterator(inputs);
    }

    private static final class MergedIterator implements SSTEntryIterator {
        private final List<? extends SSTEntryIterator> inputs;
        private final PriorityQueue<CursorEntry> heap;
        private Entry next;
        private boolean closed;

        private MergedIterator(List<? extends SSTEntryIterator> inputs) {
            if (inputs == null) {
                throw new NullPointerException("inputs must not be null");
            }
            this.inputs = List.copyOf(inputs);
            this.heap = new PriorityQueue<>(
                Comparator.comparing((CursorEntry cursor) -> cursor.entry().key())
                    .thenComparingInt(CursorEntry::sourceIndex)
            );
            for (int i = 0; i < this.inputs.size(); i++) {
                advance(i);
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (next == null) {
                next = readNext();
                if (next == null) {
                    close();
                }
            }
            return next != null;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            heap.clear();
            for (SSTEntryIterator input : inputs) {
                input.close();
            }
        }

        private Entry readNext() {
            if (heap.isEmpty()) {
                return null;
            }

            CursorEntry first = heap.poll();
            Key key = first.entry().key();
            Entry newest = first.entry();
            advance(first.sourceIndex());

            while (!heap.isEmpty() && heap.peek().entry().key().compareTo(key) == 0) {
                CursorEntry candidate = heap.poll();
                if (candidate.entry().value().sequenceId() > newest.value().sequenceId()) {
                    newest = candidate.entry();
                }
                advance(candidate.sourceIndex());
            }
            return newest;
        }

        private void advance(int sourceIndex) {
            SSTEntryIterator input = inputs.get(sourceIndex);
            if (input.hasNext()) {
                heap.add(new CursorEntry(sourceIndex, input.next()));
            }
        }
    }

    private record CursorEntry(int sourceIndex, Entry entry) {}
}
