package org.qwh.pms.lookup.direct.parquet;

import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.options.Options;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import org.apache.paimon.shade.org.apache.parquet.ParquetReadOptions;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Cached Parquet footer-derived metadata used by direct lookup. */
public final class ParquetLookupMetadata {

    private final ResolvedDataFile file;
    private final ParquetMetadata footer;
    private final List<RowGroupMetadata> rowGroups;

    private ParquetLookupMetadata(
            ResolvedDataFile file, ParquetMetadata footer, List<RowGroupMetadata> rowGroups) {
        this.file = file;
        this.footer = footer;
        this.rowGroups = rowGroups;
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

    ParquetFileReader newReader(Options options) throws IOException {
        ParquetInputFile inputFile =
                ParquetInputFile.fromPath(file.fileIO(), file.path(), file.fileSize());
        return new ParquetFileReader(
                inputFile, footer, readOptions(file, options), inputFile.newStream(), null);
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
