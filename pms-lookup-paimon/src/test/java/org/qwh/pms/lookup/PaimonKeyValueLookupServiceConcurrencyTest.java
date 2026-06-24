package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.live.CandidatePlanner;
import org.qwh.pms.lookup.live.LiveFileIndex;
import org.qwh.pms.lookup.paimon.PaimonKeyValueLookupService;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonKeyValueLookupServiceConcurrencyTest {

    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;
    private static final int BUCKET = 0;

    @Test
    void concurrentLookupAndDeltaOnlyObserveStableBucketViews() throws Exception {
        RowType keyType = RowType.of(new IntType());
        java.util.Comparator<InternalRow> comparator = new KeyComparatorSupplier(keyType).get();
        DataFileMeta first = file("first", 0L);
        DataFileMeta second = file("second", 0L);
        LiveFileIndex index = new LiveFileIndex(comparator, 4);
        index.installSnapshot(PARTITION, BUCKET, List.of(first));
        PaimonKeyValueLookupService service =
                new PaimonKeyValueLookupService(
                        index,
                        new CandidatePlanner(comparator, 0),
                        new AlwaysMissLookup(),
                        first.schemaId());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> updater =
                    executor.submit(
                            () -> {
                                await(start);
                                DataFileMeta before = first;
                                DataFileMeta after = second;
                                for (int i = 0; i < 1_000; i++) {
                                    service.applyDelta(PARTITION, BUCKET, List.of(before), List.of(after));
                                    DataFileMeta swap = before;
                                    before = after;
                                    after = swap;
                                }
                            });
            Future<?> lookups =
                    executor.submit(
                            () -> {
                                await(start);
                                for (int i = 0; i < 5_000; i++) {
                                    try {
                                        assertEquals(
                                                LookupResult.Kind.MISS,
                                                service.lookup(
                                                                PARTITION,
                                                                BUCKET,
                                                                LookupRequest.fullRow(row(5)))
                                                        .kind());
                                    } catch (java.io.IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
            start.countDown();
            updater.get(5, TimeUnit.SECONDS);
            lookups.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static DataFileMeta file(String name, long schemaId) {
        return DataFileMeta.create(
                name,
                100L,
                10L,
                row(0),
                row(9),
                null,
                null,
                0L,
                0L,
                0L,
                1,
                schemaId,
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

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class AlwaysMissLookup implements DataFileLookup {

        @Override
        public LookupResult lookup(
                FileLookupContext context, DataFileMeta file, LookupRequest request) {
            return LookupResult.miss();
        }

        @Override
        public void invalidate(FileLookupContext context, DataFileMeta file) {}
    }
}
