package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.shade.org.apache.parquet.ParquetReadOptions;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.paimon.shade.org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class DirectLookupPageIndexCacheTest {
    @TempDir java.nio.file.Path directory;

    @Test
    void realMultiGroupCompositeKeysReuseIndexesAcrossReadersAndQueries() throws Exception {
        RowType type = RowType.builder().field("tenant", DataTypes.STRING().notNull())
                .field("id", DataTypes.INT().notNull()).field("value", DataTypes.STRING()).build();
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable(directory.resolve("table"), type,
                List.of("tenant", "id"), Map.of("parquet.block.size", "8192", "parquet.page.size", "1024"))) {
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 4000; i++) rows.add(GenericRow.of(BinaryString.fromString("tenant" + i / 1000),
                    i * 2, BinaryString.fromString("payload-" + i + "x".repeat(100))));
            var files = fixture.writeRows(rows).newFiles();
            assertEquals(1, files.size());
            var file = files.get(0);
            var context = new FileLookupContext(BinaryRow.EMPTY_ROW, 0);
            var resolved = fixture.dataFileResolver().resolve(context, file);
            ParquetMetadata footer;
            try (var reader = new ParquetFileReader(ParquetInputFile.fromPath(resolved.fileIO(),
                    resolved.path(), resolved.fileSize()), ParquetReadOptions.builder().build(), null)) {
                footer = reader.getFooter();
            }
            assertTrue(footer.getBlocks().size() > 1, "exercise filtered reader row-group numbering");
            var io = new TrackingIO(footer);
            var lookup = new PaimonKeyValueParquetLookup(type, new int[]{0, 1}, fixture.schemaId(),
                    (ctx, meta) -> new ResolvedDataFile(io, resolved.path(), resolved.fileSize()));
            // Warm every row group once. HIT reads all columns; the second reader must reuse indexes.
            for (var block : footer.getBlocks()) {
                int row = Math.toIntExact(block.getRowIndexOffset());
                assertHit(lookup.lookup(context, file, request(row * 2)), row);
            }
            long offsetBytes = io.offsetBytes.get();
            assertEquals(io.totalOffsetBytes, offsetBytes, "each group's indexes read exactly once");
            assertEquals(footer.getBlocks().size(), io.offsetReads.get(), "one merged read per group");
            long columnBytes = io.columnBytes.get();
            assertTrue(columnBytes > 0);
            var executor = Executors.newFixedThreadPool(4);
            try {
                List<Callable<Void>> tasks = new ArrayList<>();
                for (int i = 0; i < 80; i++) {
                    int row = i * 49;
                    tasks.add(() -> {
                        assertHit(lookup.lookup(context, file, request(row * 2)), row);
                        assertEquals(LookupResult.Kind.MISS, lookup.lookup(context, file, request(row * 2 + 1)).kind());
                        return null;
                    });
                }
                for (var future : executor.invokeAll(tasks, 30, TimeUnit.SECONDS)) future.get();
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
            assertEquals(offsetBytes, io.offsetBytes.get());
            assertEquals(columnBytes, io.columnBytes.get(), "warm key filtering must not reload indexes");
            assertEquals(io.opens.get(), io.closes.get());

            lookup.invalidate(context, file);
            io.failIndexes = true;
            // Index failures may safely fall back to a wider scan; they must never become MISS.
            var failure = lookup.lookup(context, file, request(0));
            assertTrue(failure.kind() == LookupResult.Kind.HIT || failure.kind() == LookupResult.Kind.UNKNOWN);
            io.failIndexes = false;
            assertHit(lookup.lookup(context, file, request(0)), 0);
            assertTrue(io.offsetBytes.get() > offsetBytes, "invalidation and retry must reload indexes");
            assertEquals(io.opens.get(), io.closes.get());
        }
    }

    private static LookupRequest request(int key) {
        return LookupRequest.fullRow(GenericRow.of(BinaryString.fromString("tenant" + key / 2000), key));
    }

    private static void assertHit(LookupResult result, int row) {
        assertEquals(LookupResult.Kind.HIT, result.kind());
        assertEquals(row * 2, result.row().orElseThrow().getInt(1));
        assertEquals("payload-" + row + "x".repeat(100), result.row().orElseThrow().getString(2).toString());
    }

    private static final class TrackingIO extends LocalFileIO {
        final List<IndexReference> offsets = new ArrayList<>();
        final List<IndexReference> columns = new ArrayList<>();
        final AtomicLong offsetBytes = new AtomicLong();
        final AtomicLong columnBytes = new AtomicLong();
        final AtomicInteger offsetReads = new AtomicInteger();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final long totalOffsetBytes;
        volatile boolean failIndexes;

        TrackingIO(ParquetMetadata footer) {
            for (var block : footer.getBlocks()) for (var column : block.getColumns()) {
                if (column.getOffsetIndexReference() != null) offsets.add(column.getOffsetIndexReference());
                if (column.getColumnIndexReference() != null) columns.add(column.getColumnIndexReference());
            }
            totalOffsetBytes = offsets.stream().mapToLong(IndexReference::getLength).sum();
        }

        @Override public SeekableInputStream newInputStream(Path path) throws IOException {
            var delegate = super.newInputStream(path);
            opens.incrementAndGet();
            return new SeekableInputStream() {
                boolean closed;
                @Override public void seek(long position) throws IOException { delegate.seek(position); }
                @Override public long getPos() throws IOException { return delegate.getPos(); }
                @Override public int read() throws IOException {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
                }
                @Override public int read(byte[] target, int off, int len) throws IOException {
                    long pos = delegate.getPos();
                    if (failIndexes && (overlap(offsets, pos, len) > 0 || overlap(columns, pos, len) > 0))
                        throw new IOException("injected index failure");
                    int count = delegate.read(target, off, len);
                    if (count > 0) {
                        long offsetCount = overlap(offsets, pos, count);
                        offsetBytes.addAndGet(offsetCount);
                        if (offsetCount > 0) offsetReads.incrementAndGet();
                        columnBytes.addAndGet(overlap(columns, pos, count));
                    }
                    return count;
                }
                @Override public void close() throws IOException {
                    if (!closed) {
                        closed = true;
                        try { delegate.close(); } finally { closes.incrementAndGet(); }
                    }
                }
            };
        }
        private static long overlap(List<IndexReference> refs, long pos, int count) {
            long result = 0;
            for (var ref : refs)
                result += Math.max(0, Math.min(pos + count, ref.getOffset() + ref.getLength()) - Math.max(pos, ref.getOffset()));
            return result;
        }
    }
}
