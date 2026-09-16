package org.qwh.pms.lookup.parquet;

import org.apache.paimon.format.parquet.ParquetInputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.shade.org.apache.parquet.column.Encoding;
import org.apache.paimon.shade.org.apache.parquet.format.PageLocation;
import org.apache.paimon.shade.org.apache.parquet.format.Util;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.paimon.shade.org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.paimon.shade.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ParquetPageIndexCacheTest {
    @Test
    void mergesAllColumnsButLoadsOtherRowGroupsOnlyOnDemand() throws Exception {
        var bytes = new ByteArrayOutputStream();
        var first = group(bytes, 100, 200);
        var second = group(bytes, 300, 400);
        var cache = new ParquetPageIndexCache(List.of(first, second));
        var reads = new AtomicInteger();
        try (var input = stream(bytes.toByteArray(), reads, false)) {
            assertEquals(200, cache.offsetIndex(first.getColumns().get(1), input, unexpected()).getOffset(0));
            assertEquals(1, reads.get(), "one merged read for both columns");
            assertEquals(100, cache.offsetIndex(first.getColumns().get(0), input, unexpected()).getOffset(0));
            assertEquals(1, reads.get());
            assertEquals(400, cache.offsetIndex(second.getColumns().get(1), input, unexpected()).getOffset(0));
            assertEquals(2, reads.get(), "other row group was not preloaded");
        }
    }

    @Test
    void concurrentFirstReadsShareOneCompleteGroup() throws Exception {
        var bytes = new ByteArrayOutputStream();
        var block = group(bytes, 100, 200);
        var cache = new ParquetPageIndexCache(List.of(block));
        var reads = new AtomicInteger();
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Long>> tasks = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                tasks.add(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    try (var input = stream(bytes.toByteArray(), reads, false)) {
                        return cache.offsetIndex(block.getColumns().get(1), input, unexpected()).getOffset(0);
                    }
                });
            }
            var futures = tasks.stream().map(executor::submit).toList();
            start.countDown();
            for (var future : futures) assertEquals(200L, future.get(10, TimeUnit.SECONDS));
            assertEquals(1, reads.get());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void slowRowGroupDoesNotBlockAnotherGroup() throws Exception {
        var bytes = new ByteArrayOutputStream();
        var first = group(bytes, 100, 200);
        var second = group(bytes, 300, 400);
        var cache = new ParquetPageIndexCache(List.of(first, second));
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try (var slow = new ParquetInputStream(stream(bytes.toByteArray(), new AtomicInteger(), false).in()) {
            @Override public void readFully(byte[] target) throws IOException {
                entered.countDown();
                await(resume);
                super.readFully(target);
            }
        }; var fast = stream(bytes.toByteArray(), new AtomicInteger(), false)) {
            var blocked = executor.submit(() -> cache.offsetIndex(first.getColumns().get(0), slow, unexpected()));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var independent = executor.submit(() -> cache.offsetIndex(second.getColumns().get(0), fast, unexpected()));
            assertEquals(300, independent.get(5, TimeUnit.SECONDS).getOffset(0));
            resume.countDown();
            assertEquals(100, blocked.get(5, TimeUnit.SECONDS).getOffset(0));
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentColumnIndexLoadsPublishOneResult() throws Exception {
        var column = column("id");
        column.setColumnIndexReference(new IndexReference(10, 20));
        var cache = new ParquetPageIndexCache(List.of());
        ColumnIndex expected = ColumnIndexBuilder.build(column.getPrimitiveType(), BoundaryOrder.ASCENDING,
                List.of(false), List.of(0L),
                List.of(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(1).flip()),
                List.of(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(10).flip()));
        assertNotNull(expected);
        var attempting = new CountDownLatch(4);
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var loads = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<ColumnIndex>> tasks = new ArrayList<>();
            for (int i = 0; i < 4; i++) tasks.add(() -> {
                attempting.countDown();
                return cache.columnIndex(column, item -> {
                    loads.incrementAndGet();
                    entered.countDown();
                    await(resume);
                    return expected;
                });
            });
            var futures = tasks.stream().map(executor::submit).toList();
            assertTrue(attempting.await(5, TimeUnit.SECONDS));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(1, loads.get());
            resume.countDown();
            for (var future : futures) assertSame(expected, future.get(5, TimeUnit.SECONDS));
            assertEquals(1, loads.get());
            assertSame(expected, cache.columnIndex(column, item -> { throw new AssertionError("already cached"); }));
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void failedReadAndFailedParseDoNotPublishPartialGroup() throws Exception {
        var bytes = new ByteArrayOutputStream();
        var block = group(bytes, 100, 200);
        var cache = new ParquetPageIndexCache(List.of(block));
        var reads = new AtomicInteger();
        var key = block.getColumns().get(0);
        try (var input = stream(bytes.toByteArray(), reads, true)) {
            assertThrows(IOException.class, () -> cache.offsetIndex(key, input, unexpected()));
        }
        byte[] corrupt = bytes.toByteArray();
        int secondStart = (int) block.getColumns().get(1).getOffsetIndexReference().getOffset();
        java.util.Arrays.fill(corrupt, secondStart, corrupt.length, (byte) 0);
        try (var input = stream(corrupt, reads, false)) {
            assertThrows(IOException.class, () -> cache.offsetIndex(key, input, unexpected()));
        }
        try (var input = stream(bytes.toByteArray(), reads, false)) {
            assertEquals(100, cache.offsetIndex(key, input, unexpected()).getOffset(0));
            assertEquals(200, cache.offsetIndex(block.getColumns().get(1), input, unexpected()).getOffset(0));
        }
        assertEquals(3, reads.get(), "retry must reload the entire group");
    }

    @Test
    void scatteredAndOversizedGroupsUseOriginalLoaderAndCacheResults() throws Exception {
        for (int gap : new int[]{1, 9 * 1024 * 1024}) {
            var bytes = new ByteArrayOutputStream();
            var block = group(bytes, 100, 200);
            var first = block.getColumns().get(0);
            var second = block.getColumns().get(1);
            if (gap == 1) {
                var ref = second.getOffsetIndexReference();
                second.setOffsetIndexReference(new IndexReference(ref.getOffset() + gap, ref.getLength()));
            } else {
                first.setOffsetIndexReference(new IndexReference(0, gap));
                second.setOffsetIndexReference(new IndexReference(gap, 20));
            }
            var cache = new ParquetPageIndexCache(List.of(block));
            var loads = new AtomicInteger();
            ParquetPageIndexCache.IndexReader<OffsetIndex> loader = column -> {
                loads.incrementAndGet();
                return null;
            };
            // A null input proves the merged I/O path is not used.
            assertNull(cache.offsetIndex(first, null, loader));
            assertNull(cache.offsetIndex(second, null, loader));
            assertEquals(2, loads.get());
        }
    }

    @Test
    void absentIndexesDoNotReadAndColumnIndexFailuresRemainRetryable() throws Exception {
        var column = column("id");
        var block = new BlockMetaData();
        block.addColumn(column);
        var cache = new ParquetPageIndexCache(List.of(block));
        assertNull(cache.offsetIndex(column, null, unexpected()));
        assertNull(cache.columnIndex(column, item -> { throw new AssertionError("absent index"); }));
        column.setColumnIndexReference(new IndexReference(10, 20));
        assertThrows(IOException.class, () -> cache.columnIndex(column, item -> { throw new IOException("injected"); }));
        assertNull(cache.columnIndex(column, item -> null));
        assertNull(cache.columnIndex(column, item -> { throw new AssertionError("null result must be cached"); }));
    }

    private static BlockMetaData group(ByteArrayOutputStream bytes, long... pageOffsets) throws IOException {
        var group = new BlockMetaData();
        for (int i = 0; i < pageOffsets.length; i++) {
            var column = column("col" + i);
            int start = bytes.size();
            Util.writeOffsetIndex(new org.apache.paimon.shade.org.apache.parquet.format.OffsetIndex(
                    List.of(new PageLocation(pageOffsets[i], 50, 0))), bytes);
            column.setOffsetIndexReference(new IndexReference(start, bytes.size() - start));
            group.addColumn(column);
        }
        return group;
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new IOException("load timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static ColumnChunkMetaData column(String name) {
        return ColumnChunkMetaData.get(ColumnPath.get(name), PrimitiveTypeName.INT32,
                CompressionCodecName.UNCOMPRESSED, Set.of(Encoding.PLAIN), 0, 0, 1, 4, 4);
    }

    private static ParquetPageIndexCache.IndexReader<OffsetIndex> unexpected() {
        return column -> { throw new AssertionError("unexpected fallback"); };
    }

    private static ParquetInputStream stream(byte[] bytes, AtomicInteger reads, boolean fail) {
        return new ParquetInputStream(new SeekableInputStream() {
            int position;
            @Override public void seek(long pos) { position = Math.toIntExact(pos); }
            @Override public long getPos() { return position; }
            @Override public void close() {}
            @Override public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
            }
            @Override public int read(byte[] target, int offset, int length) throws IOException {
                reads.incrementAndGet();
                if (fail) throw new IOException("injected read failure");
                if (length == 0) return 0;
                if (position == bytes.length) return -1;
                int count = Math.min(length, bytes.length - position);
                System.arraycopy(bytes, position, target, offset, count);
                position += count;
                return count;
            }
        });
    }
}
