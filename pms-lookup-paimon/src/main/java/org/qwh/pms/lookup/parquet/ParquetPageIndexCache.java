package org.qwh.pms.lookup.parquet;

import org.apache.paimon.format.parquet.ParquetInputStream;

import org.apache.paimon.shade.org.apache.parquet.format.Util;
import org.apache.paimon.shade.org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.paimon.shade.org.apache.parquet.internal.hadoop.metadata.IndexReference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** File-scoped parsed indexes; readers and streams are never retained by this cache. */
final class ParquetPageIndexCache {
    // Bound the temporary merged read, not the lifetime size of parsed indexes.
    private static final int MAX_MERGED_BYTES = 8 * 1024 * 1024;

    private final Map<Location, OffsetGroup> offsetGroups;
    private final Map<Location, ColumnIndexEntry> columnIndexes = new ConcurrentHashMap<>();

    ParquetPageIndexCache(List<BlockMetaData> blocks) {
        Map<Location, OffsetGroup> groups = new HashMap<>();
        for (BlockMetaData block : blocks) {
            List<ColumnChunkMetaData> columns = block.getColumns().stream()
                    .filter(column -> column.getOffsetIndexReference() != null)
                    .sorted(Comparator.comparingLong(column -> column.getOffsetIndexReference().getOffset()))
                    .toList();
            OffsetGroup group = new OffsetGroup(columns);
            for (ColumnChunkMetaData column : columns) {
                groups.put(Location.of(column.getOffsetIndexReference()), group);
            }
        }
        offsetGroups = Map.copyOf(groups);
    }

    ColumnIndex columnIndex(ColumnChunkMetaData column, IndexReader<ColumnIndex> reader) throws IOException {
        IndexReference reference = column.getColumnIndexReference();
        if (reference == null) return null;
        return columnIndexes.computeIfAbsent(Location.of(reference), ignored -> new ColumnIndexEntry())
                .get(column, reader);
    }

    OffsetIndex offsetIndex(ColumnChunkMetaData column, ParquetInputStream input,
            IndexReader<OffsetIndex> reader) throws IOException {
        IndexReference reference = column.getOffsetIndexReference();
        if (reference == null) return null;
        Location location = Location.of(reference);
        OffsetGroup group = offsetGroups.get(location);
        if (group == null) return reader.read(column);
        Map<Location, OffsetIndex> indexes = group.indexes;
        if (indexes == null) indexes = group.load(input, reader);
        return indexes.get(location);
    }

    private static final class OffsetGroup {
        private final List<ColumnChunkMetaData> columns;
        private volatile Map<Location, OffsetIndex> indexes;

        private OffsetGroup(List<ColumnChunkMetaData> columns) {
            this.columns = columns;
        }

        private synchronized Map<Location, OffsetIndex> load(ParquetInputStream input, IndexReader<OffsetIndex> reader)
                throws IOException {
            if (indexes != null) return indexes;
            Map<Location, OffsetIndex> result = new HashMap<>();
            int length = mergedLength();
            if (length > 0) {
                long start = columns.get(0).getOffsetIndexReference().getOffset();
                byte[] bytes = new byte[length];
                input.seek(start);
                input.readFully(bytes);
                for (ColumnChunkMetaData column : columns) {
                    IndexReference ref = column.getOffsetIndexReference();
                    var slice = new ByteArrayInputStream(bytes, (int) (ref.getOffset() - start), ref.getLength());
                    result.put(Location.of(ref), ParquetMetadataConverter.fromParquetOffsetIndex(
                            Util.readOffsetIndex(slice)));
                }
            } else {
                // Preserve upstream handling for scattered, large or encrypted indexes.
                for (ColumnChunkMetaData column : columns) {
                    result.put(Location.of(column.getOffsetIndexReference()), reader.read(column));
                }
            }
            // Publish only a complete group. Any failure leaves indexes null for a retry.
            indexes = Collections.unmodifiableMap(result);
            return indexes;
        }

        private int mergedLength() {
            if (columns.isEmpty()) return 0;
            long start = columns.get(0).getOffsetIndexReference().getOffset();
            if (start < 0) return 0;
            long end = start;
            for (ColumnChunkMetaData column : columns) {
                IndexReference ref = column.getOffsetIndexReference();
                if (column.isEncrypted() || ref.getOffset() != end || ref.getLength() <= 0
                        || ref.getLength() > MAX_MERGED_BYTES - (end - start)
                        || end > Long.MAX_VALUE - ref.getLength()) {
                    return 0;
                }
                end += ref.getLength();
            }
            return (int) (end - start);
        }
    }

    private record Location(long offset, int length) {
        static Location of(IndexReference reference) {
            return new Location(reference.getOffset(), reference.getLength());
        }
    }

    private static final class ColumnIndexEntry {
        private ColumnIndex index;
        private volatile boolean loaded;

        ColumnIndex get(ColumnChunkMetaData column, IndexReader<ColumnIndex> reader) throws IOException {
            if (!loaded) load(column, reader);
            return index;
        }

        private synchronized void load(ColumnChunkMetaData column, IndexReader<ColumnIndex> reader)
                throws IOException {
            if (loaded) return;
            index = reader.read(column);
            // Publish the value (including a valid null) only after a successful read.
            loaded = true;
        }
    }

    @FunctionalInterface
    interface IndexReader<T> {
        T read(ColumnChunkMetaData column) throws IOException;
    }
}
