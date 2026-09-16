package org.qwh.pms.lookup.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.qwh.pms.lookup.parquet.key.LookupKeySpec;

import java.io.IOException;

/** Locates the physical row ordinal for a key inside already narrowed Parquet row ranges. */
public final class ParquetKeyRowLocator {

    private final LookupKeySpec keySpec;
    private final Options options;
    private final int batchSize;

    public ParquetKeyRowLocator(LookupKeySpec keySpec, Options options, int batchSize) {
        this.keySpec = keySpec;
        this.options = options;
        this.batchSize = batchSize;
    }

    public long locate(
            ParquetLookupMetadata metadata, RowRangeCandidate rowRange, InternalRow key)
            throws IOException {
        RowType keyReadType = keySpec.keyType();
        RoaringBitmap32 selection = ParquetReaderSupport.rowRangeSelection(rowRange);

        try (FileRecordReader<InternalRow> reader = newReader(metadata, keyReadType, selection)) {
            long rowIndex = rowRange.fromInclusive();
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    if (keySpec.keyComparator().compare(row, key) == 0) {
                        batch.releaseBatch();
                        return rowIndex;
                    }
                    rowIndex++;
                }
                batch.releaseBatch();
            }
        }
        return -1L;
    }

    private FileRecordReader<InternalRow> newReader(
            ParquetLookupMetadata metadata, RowType readType, RoaringBitmap32 selection) throws IOException {
        return ParquetReaderSupport.newReader(metadata, readType, selection, options, batchSize);
    }
}
