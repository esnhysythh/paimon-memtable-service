package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveBucketView;
import org.qwh.pms.lookup.view.LiveFileIndex;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveFileIndexTest {

    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;
    private static final int BUCKET = 0;
    private static final RowType KEY_TYPE = RowType.of(new IntType());
    private static final Comparator<InternalRow> KEY_COMPARATOR =
            new KeyComparatorSupplier(KEY_TYPE).get();

    private final LiveFileIndex index = new LiveFileIndex(KEY_COMPARATOR, 4);
    private final CandidatePlanner planner = new CandidatePlanner(KEY_COMPARATOR, 0);

    @Test
    void installedSnapshotCreatesBucketViewAndCandidates() {
        DataFileMeta l1a = file("l1-a", 1, 0, 9, 1, 1);
        DataFileMeta l1b = file("l1-b", 1, 10, 19, 2, 2);

        index.installSnapshot(PARTITION, BUCKET, List.of(l1b, l1a));

        LiveBucketView view = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertTrue(view.valid());
        assertFileNames(List.of("l1-b"), planner.plan(view, row(15)));
        assertFileNames(List.of(), planner.plan(view, row(20)));
    }

    @Test
    void level0CandidatesKeepNewestFirstOrderWhenRangesOverlap() {
        DataFileMeta oldL0 = file("old-l0", 0, 0, 20, 1, 10);
        DataFileMeta newL0 = file("new-l0", 0, 5, 15, 11, 20);

        index.installSnapshot(PARTITION, BUCKET, List.of(oldL0, newL0));

        LiveBucketView view = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertFileNames(List.of("new-l0", "old-l0"), planner.plan(view, row(10)));
        assertFileNames(List.of("old-l0"), planner.plan(view, row(2)));
    }

    @Test
    void plannerReturnsAtMostOneCandidatePerSortedLevel() {
        DataFileMeta l1a = file("l1-a", 1, 0, 9, 1, 1);
        DataFileMeta l1b = file("l1-b", 1, 10, 19, 2, 2);
        DataFileMeta l2 = file("l2", 2, 0, 30, 3, 3);

        index.installSnapshot(PARTITION, BUCKET, List.of(l2, l1b, l1a));

        LiveBucketView view = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertFileNames(List.of("l1-b", "l2"), planner.plan(view, row(15)));
    }

    @Test
    void deltaReplacesCompactedFiles() {
        DataFileMeta beforeA = file("before-a", 1, 0, 9, 1, 1);
        DataFileMeta beforeB = file("before-b", 1, 10, 19, 2, 2);
        DataFileMeta after = file("after", 1, 0, 19, 3, 3);

        index.installSnapshot(PARTITION, BUCKET, List.of(beforeA, beforeB));
        index.applyDelta(PARTITION, BUCKET, List.of(beforeA, beforeB), List.of(after));

        LiveBucketView view = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertFileNames(List.of("after"), planner.plan(view, row(4)));
        assertFileNames(List.of("after"), planner.plan(view, row(18)));
    }

    @Test
    void removingUnknownFileInvalidatesBucket() {
        DataFileMeta live = file("live", 1, 0, 9, 1, 1);
        DataFileMeta unknown = file("unknown", 1, 10, 19, 2, 2);

        index.installSnapshot(PARTITION, BUCKET, List.of(live));

        assertThrows(
                IllegalStateException.class,
                () -> index.applyDelta(PARTITION, BUCKET, List.of(unknown), List.of()));

        LiveBucketView view = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertFalse(view.valid());
        assertFileNames(List.of(), planner.plan(view, row(5)));
    }

    @Test
    void invalidBucketRequiresFullSnapshotInsteadOfLaterDelta() {
        DataFileMeta live = file("live", 1, 0, 9, 1, 1);
        DataFileMeta unknown = file("unknown", 1, 10, 19, 2, 2);
        DataFileMeta later = file("later", 1, 10, 19, 3, 3);

        index.installSnapshot(PARTITION, BUCKET, List.of(live));
        assertThrows(
                IllegalStateException.class,
                () -> index.applyDelta(PARTITION, BUCKET, List.of(unknown), List.of()));

        assertThrows(
                IllegalStateException.class,
                () -> index.applyDelta(PARTITION, BUCKET, List.of(), List.of(later)));
        assertFalse(index.bucketView(PARTITION, BUCKET).orElseThrow().valid());

        index.installSnapshot(PARTITION, BUCKET, List.of(live, later));
        LiveBucketView recovered = index.bucketView(PARTITION, BUCKET).orElseThrow();
        assertTrue(recovered.valid());
        assertFileNames(List.of("live"), planner.plan(recovered, row(5)));
        assertFileNames(List.of("later"), planner.plan(recovered, row(15)));
    }

    @Test
    void previouslyPublishedViewRemainsStableAfterDelta() {
        DataFileMeta before = file("before", 1, 0, 9, 1, 1);
        DataFileMeta after = file("after", 1, 10, 19, 2, 2);

        index.installSnapshot(PARTITION, BUCKET, List.of(before));
        LiveBucketView first = index.bucketView(PARTITION, BUCKET).orElseThrow();

        index.applyDelta(PARTITION, BUCKET, List.of(before), List.of(after));
        LiveBucketView second = index.bucketView(PARTITION, BUCKET).orElseThrow();

        assertTrue(second.version() > first.version());
        assertFileNames(List.of("before"), planner.plan(first, row(5)));
        assertFileNames(List.of(), planner.plan(first, row(15)));
        assertFileNames(List.of(), planner.plan(second, row(5)));
        assertFileNames(List.of("after"), planner.plan(second, row(15)));
    }

    private static void assertFileNames(List<String> expected, List<DataFileMeta> actual) {
        assertEquals(expected, actual.stream().map(DataFileMeta::fileName).toList());
    }

    private static DataFileMeta file(
            String name, int level, int minKey, int maxKey, long minSeq, long maxSeq) {
        return DataFileMeta.create(
                name,
                100L,
                10L,
                row(minKey),
                row(maxKey),
                null,
                null,
                minSeq,
                maxSeq,
                0L,
                level,
                0L,
                null,
                null,
                null,
                null,
                null);
    }

    private static BinaryRow row(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
