package org.qwh.pms.lookup.parquet;

import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.RoaringBitmap32;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import org.apache.paimon.shade.org.apache.parquet.ParquetReadOptions;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Cached Parquet footer and page indexes used by direct lookup. */
public final class ParquetLookupMetadata {

    private final ResolvedDataFile file;
    private final ParquetMetadata footer;
    private final List<RowGroupMetadata> rowGroups;
    private final ParquetPageIndexCache pageIndexes;

    private ParquetLookupMetadata(
            ResolvedDataFile file, ParquetMetadata footer, List<RowGroupMetadata> rowGroups) {
        this.file = file;
        this.footer = footer;
        this.rowGroups = rowGroups;
        this.pageIndexes = new ParquetPageIndexCache(footer.getBlocks());
    }

    static ParquetLookupMetadata load(ResolvedDataFile file, Options options) throws IOException {
        ParquetInputFile inputFile =
                ParquetInputFile.fromPath(file.fileIO(), file.path(), file.fileSize());
        ParquetReadOptions readOptions = readOptions(file, options);
        try (ParquetFileReader reader = new ParquetFileReader(inputFile, readOptions, null)) {
            ParquetMetadata footer = reader.getFooter();
            return new ParquetLookupMetadata(file, footer, rowGroups(footer.getBlocks()));
        }
    }

    ResolvedDataFile file() {
        return file;
    }

    ParquetFileReader newReader(Options options) throws IOException {
        return newReader(options, null);
    }

    // Share parsed metadata only. Each query owns its stream, selection and reader state.
    ParquetFileReader newReader(Options options, RoaringBitmap32 selection) throws IOException {
        ParquetInputFile inputFile =
                ParquetInputFile.fromPath(file.fileIO(), file.path(), file.fileSize());
        // These public methods are upstream internal APIs; keep this adapter limited to indexes.
        return new ParquetFileReader(
                inputFile, footer, readOptions(file, options), inputFile.newStream(), selection) {
            @Override
            public ColumnIndex readColumnIndex(ColumnChunkMetaData column) throws IOException {
                return pageIndexes.columnIndex(column, item -> super.readColumnIndex(item));
            }

            @Override
            public OffsetIndex readOffsetIndex(ColumnChunkMetaData column) throws IOException {
                return pageIndexes.offsetIndex(column, f, item -> super.readOffsetIndex(item));
            }
        };
    }

    List<RowGroupMetadata> rowGroups() {
        return rowGroups;
    }

    private static ParquetReadOptions readOptions(ResolvedDataFile file, Options options) {
        return ParquetUtil.getParquetReadOptionsBuilder(options)
                .withRange(0, file.fileSize())
                .build();
    }

    private static List<RowGroupMetadata> rowGroups(List<BlockMetaData> blocks) {
        List<RowGroupMetadata> rowGroups = new ArrayList<>(blocks.size());
        long fallbackOffset = 0L;
        for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
            BlockMetaData block = blocks.get(blockIndex);
            long rowIndexOffset =
                    block.getRowIndexOffset() >= 0 ? block.getRowIndexOffset() : fallbackOffset;
            rowGroups.add(
                    new RowGroupMetadata(
                            blockIndex, block, rowIndexOffset, columnPaths(block)));
            fallbackOffset += block.getRowCount();
        }
        return List.copyOf(rowGroups);
    }

    private static Set<ColumnPath> columnPaths(BlockMetaData block) {
        Set<ColumnPath> paths = new LinkedHashSet<>();
        block.getColumns().forEach(column -> paths.add(column.getPath()));
        return Set.copyOf(paths);
    }

    record RowGroupMetadata(
            int blockIndex,
            BlockMetaData block,
            long rowIndexOffset,
            Set<ColumnPath> columnPaths) {}
}
