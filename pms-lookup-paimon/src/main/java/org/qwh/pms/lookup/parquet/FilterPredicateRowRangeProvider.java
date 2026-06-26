package org.qwh.pms.lookup.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.qwh.pms.lookup.parquet.key.LookupKeySpec;

import org.apache.paimon.shade.org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.paimon.shade.org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.paimon.shade.org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.paimon.shade.org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Row range provider backed by Parquet's native FilterPredicate column-index filtering. */
public final class FilterPredicateRowRangeProvider {

    private final LookupKeySpec keySpec;
    private final Options options;

    public FilterPredicateRowRangeProvider(LookupKeySpec keySpec, Options options) {
        this.keySpec = keySpec;
        this.options = options;
    }

    public List<RowRangeCandidate> candidateRowRanges(
            ParquetLookupMetadata metadata, InternalRow key) throws IOException {
        FilterPredicate predicate = ParquetFilterPredicateBuilder.build(keySpec, key);
        FilterCompat.Filter filter = FilterCompat.get(predicate);
        List<RowRangeCandidate> candidates = new ArrayList<>();

        try (ParquetFileReader reader = metadata.newReader(options)) {
            for (ParquetLookupMetadata.RowGroupMetadata rowGroup : metadata.rowGroups()) {
                BlockMetaData block = rowGroup.block();
                if (!canDrop(predicate, block)) {
                    addRanges(candidates, rowGroup, filter, reader);
                }
            }
        }
        return candidates;
    }

    private static boolean canDrop(FilterPredicate predicate, BlockMetaData block) {
        try {
            return StatisticsFilter.canDrop(predicate, block.getColumns());
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void addRanges(
            List<RowRangeCandidate> candidates,
            ParquetLookupMetadata.RowGroupMetadata rowGroup,
            FilterCompat.Filter filter,
            ParquetFileReader reader) {
        BlockMetaData block = rowGroup.block();
        long rowIndexOffset = rowGroup.rowIndexOffset();
        try {
            RowRanges rowRanges =
                    ColumnIndexFilter.calculateRowRanges(
                            filter,
                            reader.getColumnIndexStore(rowGroup.blockIndex()),
                            rowGroup.columnPaths(),
                            block.getRowCount());
            for (RowRanges.Range range : rowRanges.getRanges()) {
                candidates.add(
                        new RowRangeCandidate(
                                rowIndexOffset + range.from, rowIndexOffset + range.to + 1));
            }
        } catch (RuntimeException e) {
            candidates.add(
                    new RowRangeCandidate(rowIndexOffset, rowIndexOffset + block.getRowCount()));
        }
    }
}
