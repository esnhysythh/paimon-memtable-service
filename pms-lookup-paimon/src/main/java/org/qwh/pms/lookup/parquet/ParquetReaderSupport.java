package org.qwh.pms.lookup.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.parquet.ParquetReaderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.paimon.shade.org.apache.parquet.filter2.compat.FilterCompat;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import java.io.IOException;

/** Shared reader helpers for direct Parquet lookup paths. */
public final class ParquetReaderSupport {

    private ParquetReaderSupport() {}

    public static FileRecordReader<InternalRow> newReader(
            ResolvedDataFile file,
            RowType readType,
            RoaringBitmap32 selection,
            Options options,
            int batchSize)
            throws IOException {
        ParquetReaderFactory readerFactory =
                new ParquetReaderFactory(options, readType, batchSize, FilterCompat.NOOP);
        return readerFactory.createReader(
                new FormatReaderContext(
                        file.fileIO(), file.path(), file.fileSize(), selection));
    }

    public static RoaringBitmap32 rowRangeSelection(RowRangeCandidate rowRange) throws IOException {
        ensureSupportedRowIndex(rowRange.fromInclusive());
        if (rowRange.toExclusive() > Integer.MAX_VALUE) {
            throw new IOException("Row range exceeds 32-bit selection limit: " + rowRange);
        }
        return RoaringBitmap32.bitmapOfRange(rowRange.fromInclusive(), rowRange.toExclusive());
    }

    public static RoaringBitmap32 singleRowSelection(long rowIndex) throws IOException {
        ensureSupportedRowIndex(rowIndex);
        return RoaringBitmap32.bitmapOf((int) rowIndex);
    }

    public static void ensureSupportedRowIndex(long rowIndex) throws IOException {
        if (rowIndex < 0 || rowIndex > Integer.MAX_VALUE) {
            throw new IOException("Row index exceeds 32-bit selection limit: " + rowIndex);
        }
    }
}
