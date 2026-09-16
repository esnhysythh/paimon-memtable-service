package org.qwh.pms.lookup.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.parquet.reader.VectorizedParquetRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.io.ColumnIOFactory;
import org.apache.paimon.shade.org.apache.parquet.schema.MessageType;
import org.apache.paimon.shade.org.apache.parquet.schema.Type;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import java.io.IOException;

import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.buildFieldsList;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.createWritableColumnVector;

/** Shared reader helpers for direct Parquet lookup paths. */
public final class ParquetReaderSupport {

    private ParquetReaderSupport() {}

    public static FileRecordReader<InternalRow> newReader(
            ParquetLookupMetadata metadata,
            RowType readType,
            RoaringBitmap32 selection,
            Options options,
            int batchSize)
            throws IOException {
        // Paimon 1.4.1's factory cannot accept a cached footer. Assemble its existing
        // vectorized reader here; page reading and decoding remain Paimon's responsibility.
        // PMS reads whole top-level fields under a fixed schema, so retain each selected
        // field's physical subtree (including nested types/IDs), without schema evolution
        // or arbitrary nested projection support.
        ParquetFileReader reader = metadata.newReader(options, selection);
        try {
            ResolvedDataFile file = metadata.file();
            MessageType fileSchema = reader.getFileMetaData().getSchema();
            DataField[] fields = readType.getFields().toArray(new DataField[0]);
            Type[] selectedTypes = new Type[fields.length];
            for (int i = 0; i < fields.length; i++) {
                String name = fields[i].name();
                if (!fileSchema.containsField(name)) {
                    throw new IOException("Missing field in fixed-schema Parquet file: " + name);
                }
                selectedTypes[i] = fileSchema.getType(name);
            }
            MessageType requestedSchema = new MessageType(fileSchema.getName(), selectedTypes);
            var columnIO = new ColumnIOFactory().getColumnIO(requestedSchema);
            var parquetFields = buildFieldsList(fields, columnIO, requestedSchema);
            reader.setRequestedSchema(requestedSchema);
            WritableColumnVector[] vectors = new WritableColumnVector[fields.length];
            for (int i = 0; i < fields.length; i++) {
                vectors[i] = createWritableColumnVector(batchSize, fields[i].type());
            }
            return new VectorizedParquetRecordReader(
                    file.path(), reader, fileSchema, parquetFields, vectors, batchSize, file.fileIO());
        } catch (IOException | RuntimeException | Error failure) {
            try {
                reader.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
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
