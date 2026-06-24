package org.qwh.pms.lookup.local;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.ProjectedRow;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupRequestValidator;
import org.qwh.pms.lookup.api.LookupResult;

import java.io.File;
import java.io.IOException;

/** VALUE_SST cache entry backed by a Paimon lookup store reader. */
public final class ValueSstCacheEntry implements LocalCacheEntry {

    private final String fileName;
    private final int level;
    private final File localFile;
    private final RowType keyType;
    private final RowType valueType;
    private final RowCompactedSerializer keySerializer;
    private final PersistProcessor<KeyValue> valueProcessor;
    private final LookupStoreReader reader;
    private boolean closed;

    ValueSstCacheEntry(
            DataFileMeta file,
            File localFile,
            RowType keyType,
            RowType valueType,
            LookupSerializerFactory serializerFactory,
            String serializerVersion,
            LookupStoreReader reader) {
        this.fileName = file.fileName();
        this.level = file.level();
        this.localFile = localFile;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueProcessor =
                PersistValueProcessor.factory(valueType)
                        .create(serializerVersion, serializerFactory, null);
        this.reader = reader;
    }

    @Override
    public LocalCacheMode mode() {
        return LocalCacheMode.VALUE_SST;
    }

    @Override
    public synchronized LookupResult lookup(LookupRequest request) throws IOException {
        ensureOpen();
        LookupRequestValidator.ensureKeyFieldCount(request, keyType.getFieldCount(), "key type");
        LookupRequestValidator.ensureNonNullKey(request);

        byte[] keyBytes = keySerializer.serializeToBytes(request.key());
        byte[] valueBytes = reader.lookup(keyBytes);
        if (valueBytes == null) {
            return LookupResult.miss();
        }

        KeyValue keyValue =
                valueProcessor.readFromDisk(request.key(), level, valueBytes, fileName);
        if (keyValue.valueKind().isRetract()) {
            return LookupResult.deleted();
        }
        return LookupResult.hit(copy(keyValue.value(), request), -1L);
    }

    @Override
    public long sizeBytes() {
        return localFile.length();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            reader.close();
        } finally {
            closed = true;
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }

    File localFile() {
        return localFile;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("VALUE_SST cache entry is closed: " + localFile);
        }
    }

    private InternalRow copy(InternalRow value, LookupRequest request) {
        return request.projection()
                .map(
                        projection -> {
                            RowType projectedType = valueType.project(projection);
                            ProjectedRow projectedRow = ProjectedRow.from(projection).replaceRow(value);
                            return new InternalRowSerializer(projectedType).copy(projectedRow);
                        })
                .orElseGet(() -> new InternalRowSerializer(valueType).copy(value));
    }
}
