package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectLookupFooterReuseTest {
    private static final FileLookupContext CONTEXT = new FileLookupContext(BinaryRow.EMPTY_ROW, 0);
    private static final RowType ROW_TYPE = RowType.builder()
            .field("id", DataTypes.INT().notNull())
            .field("payload", DataTypes.STRING()).build();

    @Test
    void warmConcurrentHitsAndMissesDoNotReadFooterAndInvalidationReloadsIt() throws Exception {
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable(
                "footer-reuse", ROW_TYPE, List.of("id"))) {
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 512; i++) {
                rows.add(GenericRow.of(i * 2, BinaryString.fromString("value-" + i)));
            }
            var file = fixture.writeRows(rows).newFiles().get(0);
            var resolved = fixture.dataFileResolver().resolve(CONTEXT, file);
            var io = new TrackingFileIO(resolved);
            var lookup = new PaimonKeyValueParquetLookup(ROW_TYPE, new int[]{0}, fixture.schemaId(),
                    (context, meta) -> new ResolvedDataFile(io, resolved.path(), resolved.fileSize()));
            var retained = lookup.lookup(CONTEXT, file, LookupRequest.fullRow(GenericRow.of(0)))
                    .row().orElseThrow();
            assertTrue(io.footerBytes.get() > 0, "cold lookup must load the footer");
            io.footerBytes.set(0);
            var executor = Executors.newFixedThreadPool(4);
            try {
                List<Callable<Void>> tasks = new ArrayList<>();
                for (int i = 0; i < 32; i++) {
                    int index = i * 15;
                    tasks.add(() -> {
                        var hit = lookup.lookup(CONTEXT, file,
                                LookupRequest.fullRow(GenericRow.of(index * 2)));
                        assertEquals(LookupResult.Kind.HIT, hit.kind());
                        assertEquals("value-" + index, hit.row().orElseThrow().getString(1).toString());
                        assertEquals(LookupResult.Kind.MISS, lookup.lookup(CONTEXT, file,
                                LookupRequest.fullRow(GenericRow.of(index * 2 + 1))).kind());
                        return null;
                    });
                }
                for (var future : executor.invokeAll(tasks, 30, TimeUnit.SECONDS)) future.get();
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
            assertEquals("value-0", retained.getString(1).toString(), "later readers must not mutate a returned row");
            assertEquals(0, io.footerBytes.get(), "warm readers must never read the footer region");
            assertEquals(io.opens.get(), io.closes.get());

            lookup.invalidate(CONTEXT, file);
            assertEquals(LookupResult.Kind.HIT, lookup.lookup(CONTEXT, file,
                    LookupRequest.fullRow(GenericRow.of(0))).kind());
            assertTrue(io.footerBytes.get() > 0, "invalidation must allow a fresh footer load");

            io.failReads = true;
            assertEquals(LookupResult.Kind.UNKNOWN, lookup.lookup(CONTEXT, file,
                    LookupRequest.fullRow(GenericRow.of(0))).kind());
            assertEquals(io.opens.get(), io.closes.get(), "failed reads must close all streams");
        }
    }

    @Test
    void readsWholeNestedFieldsNullsAndProjectedValues() throws Exception {
        RowType nested = new RowType(List.of(new DataField(10, "label", DataTypes.STRING()),
                new DataField(11, "numbers", DataTypes.ARRAY(DataTypes.INT()))));
        RowType mapValue = new RowType(List.of(new DataField(20, "label", DataTypes.STRING()),
                new DataField(21, "numbers", DataTypes.ARRAY(DataTypes.INT()))));
        RowType rowType = RowType.builder().field("id", DataTypes.INT().notNull())
                .field("details", nested)
                .field("attributes", DataTypes.MAP(DataTypes.STRING(), mapValue))
                .field("amount", DataTypes.DECIMAL(18, 3))
                .field("created", DataTypes.TIMESTAMP(6))
                .field("bytes", DataTypes.BYTES()).build();
        GenericRow detail = GenericRow.of(BinaryString.fromString("nested"),
                new GenericArray(new Integer[]{1, null, 3}));
        GenericRow expected = GenericRow.of(0, detail,
                new GenericMap(Map.of(BinaryString.fromString("a"), detail)),
                Decimal.fromBigDecimal(new BigDecimal("123.456"), 18, 3),
                Timestamp.fromLocalDateTime(LocalDateTime.of(2026, 9, 15, 12, 30, 0, 123456000)),
                new byte[]{0, 1, -1});
        GenericRow nulls = GenericRow.of(2, null, null, null, null, null);
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable("footer-nested", rowType, List.of("id"))) {
            var file = fixture.writeRows(List.of(expected, nulls)).newFiles().get(0);
            var lookup = new PaimonKeyValueParquetLookup(rowType, new int[]{0}, fixture.schemaId(), fixture.dataFileResolver());
            var serializer = new InternalRowSerializer(rowType);
            for (InternalRow row : List.of(expected, nulls)) {
                var actual = lookup.lookup(CONTEXT, file, LookupRequest.fullRow(GenericRow.of(row.getInt(0))));
                assertEquals(LookupResult.Kind.HIT, actual.kind());
                assertEquals(serializer.toBinaryRow(row).copy(), serializer.toBinaryRow(actual.row().orElseThrow()).copy());
            }
            var projected = lookup.lookup(CONTEXT, file, LookupRequest.projected(GenericRow.of(0), new int[]{2, 1}));
            var projectedSerializer = new InternalRowSerializer(rowType.project(new int[]{2, 1}));
            assertEquals(projectedSerializer.toBinaryRow(GenericRow.of(expected.getField(2), detail)).copy(),
                    projectedSerializer.toBinaryRow(projected.row().orElseThrow()).copy());
        }
    }

    @Test
    void assemblyFailureClosesStreamAndReturnsUnknown() throws Exception {
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable("footer-invalid-schema", ROW_TYPE, List.of("id"))) {
            var file = fixture.writeRows(List.of(GenericRow.of(0, BinaryString.fromString("value")))).newFiles().get(0);
            var resolved = fixture.dataFileResolver().resolve(CONTEXT, file);
            var io = new TrackingFileIO(resolved);
            RowType invalidType = RowType.builder().field("id", DataTypes.INT().notNull())
                    .field("missing", DataTypes.STRING()).build();
            var lookup = new PaimonKeyValueParquetLookup(invalidType, new int[]{0}, fixture.schemaId(),
                    (context, meta) -> new ResolvedDataFile(io, resolved.path(), resolved.fileSize()));
            assertEquals(LookupResult.Kind.UNKNOWN, lookup.lookup(CONTEXT, file,
                    LookupRequest.fullRow(GenericRow.of(0))).kind());
            assertEquals(io.opens.get(), io.closes.get());
        }
    }

    /** Counts actual reads overlapping the footer, independently of the reader implementation. */
    private static final class TrackingFileIO extends LocalFileIO {
        private final long footerStart;
        private final AtomicLong footerBytes = new AtomicLong();
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private volatile boolean failReads;

        private TrackingFileIO(ResolvedDataFile file) throws IOException {
            try (var input = new RandomAccessFile(toFile(file.path()), "r")) {
                input.seek(file.fileSize() - 8);
                footerStart = file.fileSize() - 8 - Integer.toUnsignedLong(Integer.reverseBytes(input.readInt()));
            }
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            var delegate = super.newInputStream(path);
            opens.incrementAndGet();
            return new SeekableInputStream() {
                private boolean closed;

                @Override
                public void seek(long position) throws IOException { delegate.seek(position); }

                @Override
                public long getPos() throws IOException { return delegate.getPos(); }

                @Override
                public int read() throws IOException {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    if (failReads) throw new IOException("injected read failure");
                    long start = delegate.getPos();
                    int count = delegate.read(bytes, offset, length);
                    if (count > 0) footerBytes.addAndGet(Math.max(0, start + count - Math.max(start, footerStart)));
                    return count;
                }

                @Override
                public void close() throws IOException {
                    if (!closed) {
                        closed = true;
                        try { delegate.close(); } finally { closes.incrementAndGet(); }
                    }
                }
            };
        }
    }
}
