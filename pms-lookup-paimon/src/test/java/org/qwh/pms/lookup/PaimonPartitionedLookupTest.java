package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.direct.parquet.PaimonKeyValueDirectLookup;
import org.qwh.pms.lookup.live.CandidatePlanner;
import org.qwh.pms.lookup.live.LiveFileIndex;
import org.qwh.pms.lookup.local.LocalCacheBuildContext;
import org.qwh.pms.lookup.local.LocalCacheDirectory;
import org.qwh.pms.lookup.paimon.PaimonKeyValueLookupService;

import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonPartitionedLookupTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("region", DataTypes.INT())
                    .field("order_id", DataTypes.BIGINT())
                    .field("amount", DataTypes.INT())
                    .build();
    private static final int[] TRIMMED_KEY_FIELDS = {1};
    private static final RowType FILE_KEY_TYPE =
            PrimaryKeyTableUtils.addKeyNamePrefix(ROW_TYPE.project(TRIMMED_KEY_FIELDS));
    private static final Comparator<InternalRow> FILE_KEY_COMPARATOR =
            new KeyComparatorSupplier(FILE_KEY_TYPE).get();

    @Test
    void lookupUsesPartitionBucketContextForLiveFilesAndCacheIdentity() throws Exception {
        try (PaimonTableFixture fixture =
                        PaimonTableFixture.createPrimaryKeyTable(
                                "orders-partitioned-lookup",
                                ROW_TYPE,
                                List.of("region"),
                                List.of("region", "order_id"),
                                2);
                LocalCacheDirectory cacheDirectory =
                        new LocalCacheDirectory(Files.createTempDirectory("partitioned-lookup-cache-"))) {
            GenericRow firstBucketRow = row(1, 1L, 101);
            int firstBucket = fixture.bucketOf(firstBucketRow);
            GenericRow secondBucketRow =
                    findRowInDifferentBucket(fixture, 1, firstBucket, 102);
            int secondBucket = fixture.bucketOf(secondBucketRow);
            GenericRow sameKeyOtherPartition = row(2, 1L, 201);

            PaimonTableFixture.WriteResult writeResult =
                    fixture.writeRows(List.of(firstBucketRow, secondBucketRow, sameKeyOtherPartition));
            assertTrue(
                    writeResult.bucketDeltas().stream()
                                    .map(PaimonTableFixture.BucketDelta::bucket)
                                    .distinct()
                                    .count()
                            >= 2);

            PaimonKeyValueLookupService service =
                    new PaimonKeyValueLookupService(
                            new LiveFileIndex(FILE_KEY_COMPARATOR, 4),
                            new CandidatePlanner(FILE_KEY_COMPARATOR, 0),
                            new PaimonKeyValueDirectLookup(
                                    ROW_TYPE,
                                    TRIMMED_KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()),
                            fixture.schemaId());
            for (PaimonTableFixture.BucketDelta delta : writeResult.bucketDeltas()) {
                service.installSnapshot(delta.partition(), delta.bucket(), delta.addedFiles());
            }

            assertHit(service, partition(1), firstBucket, 1L, 101);
            assertHit(service, partition(1), secondBucket, secondBucketRow.getLong(1), 102);
            assertHit(service, partition(2), firstBucket, 1L, 201);

            DataFileMeta file = writeResult.bucketDeltas().get(0).addedFiles().get(0);
            assertNotEquals(
                    cacheDirectory
                            .cacheFile(file, new LocalCacheBuildContext(partition(1), firstBucket))
                            .toPath(),
                    cacheDirectory
                            .cacheFile(file, new LocalCacheBuildContext(partition(2), firstBucket))
                            .toPath());
        }
    }

    private static GenericRow findRowInDifferentBucket(
            PaimonTableFixture fixture, int region, int excludedBucket, int amount) throws Exception {
        for (long orderId = 2L; orderId < 100L; orderId++) {
            GenericRow candidate = row(region, orderId, amount);
            if (fixture.bucketOf(candidate) != excludedBucket) {
                return candidate;
            }
        }
        throw new AssertionError("Unable to produce rows for both fixed buckets");
    }

    private static void assertHit(
            PaimonKeyValueLookupService service,
            BinaryRow partition,
            int bucket,
            long orderId,
            int expectedAmount)
            throws Exception {
        LookupResult result =
                service.lookup(partition, bucket, LookupRequest.projected(key(orderId), new int[] {2}));
        assertEquals(LookupResult.Kind.HIT, result.kind());
        assertEquals(expectedAmount, result.row().orElseThrow().getInt(0));
    }

    private static GenericRow row(int region, long orderId, int amount) {
        return GenericRow.of(region, orderId, amount);
    }

    private static BinaryRow partition(int region) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, region);
        writer.complete();
        return partition;
    }

    private static BinaryRow key(long orderId) {
        BinaryRow key = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(key);
        writer.writeLong(0, orderId);
        writer.complete();
        return key;
    }
}
