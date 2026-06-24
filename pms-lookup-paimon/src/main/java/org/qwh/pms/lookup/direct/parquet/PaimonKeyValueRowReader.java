package org.qwh.pms.lookup.direct.parquet;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.qwh.pms.lookup.api.ResolvedDataFile;
import java.io.IOException;

/** Reads one physical Paimon KeyValue row after the key locator has found its row ordinal. */
final class PaimonKeyValueRowReader {

    private final RowType keyType;
    private final RowType valueType;
    private final RowType physicalRowType;
    private final Options options;
    private final int batchSize;

    PaimonKeyValueRowReader(
            RowType keyType,
            RowType valueType,
            RowType physicalRowType,
            Options options,
            int batchSize) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.physicalRowType = physicalRowType;
        this.options = options;
        this.batchSize = batchSize;
    }

    KeyValue read(ResolvedDataFile file, long rowIndex) throws IOException {
        RoaringBitmap32 selection = ParquetReaderSupport.singleRowSelection(rowIndex);
        KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);
        try (FileRecordReader<InternalRow> reader =
                ParquetReaderSupport.newReader(
                        file, physicalRowType, selection, options, batchSize)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    serializer.fromRow(row);
                    KeyValue copy = serializer.getCopiedKv();
                    batch.releaseBatch();
                    return copy;
                }
                batch.releaseBatch();
            }
        }
        throw new IOException("Selected KeyValue was not found in Parquet file: rowIndex=" + rowIndex);
    }
}
