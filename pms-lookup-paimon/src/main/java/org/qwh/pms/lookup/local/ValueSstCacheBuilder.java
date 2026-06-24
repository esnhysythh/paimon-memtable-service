package org.qwh.pms.lookup.local;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Builds VALUE_SST local cache entries by scanning one Paimon KeyValue data file. */
public final class ValueSstCacheBuilder implements LocalCacheBuilder {

    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueDataFileReaderFactory readerFactory;
    private final LookupStoreFactory lookupStoreFactory;
    private final LookupSerializerFactory serializerFactory;
    private final Function<Long, BloomFilter.Builder> bloomFilterFactory;
    private final BiFunction<DataFileMeta, LocalCacheBuildContext, File> localFileFactory;

    public ValueSstCacheBuilder(
            RowType keyType,
            RowType valueType,
            KeyValueDataFileReaderFactory readerFactory,
            LookupStoreFactory lookupStoreFactory,
            LookupSerializerFactory serializerFactory,
            Function<Long, BloomFilter.Builder> bloomFilterFactory,
            LocalCacheDirectory localCacheDirectory) {
        this(
                keyType,
                valueType,
                readerFactory,
                lookupStoreFactory,
                serializerFactory,
                bloomFilterFactory,
                Objects.requireNonNull(localCacheDirectory, "localCacheDirectory")::cacheFile);
    }

    public ValueSstCacheBuilder(
            RowType keyType,
            RowType valueType,
            KeyValueDataFileReaderFactory readerFactory,
            LookupStoreFactory lookupStoreFactory,
            LookupSerializerFactory serializerFactory,
            Function<Long, BloomFilter.Builder> bloomFilterFactory,
            BiFunction<DataFileMeta, LocalCacheBuildContext, File> localFileFactory) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.readerFactory = readerFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.serializerFactory = serializerFactory;
        this.bloomFilterFactory = bloomFilterFactory;
        this.localFileFactory = localFileFactory;
    }

    @Override
    public LocalCacheMode mode() {
        return LocalCacheMode.VALUE_SST;
    }

    @Override
    public LocalCacheEntry build(DataFileMeta file, LocalCacheBuildContext context)
            throws IOException {
        Objects.requireNonNull(file, "file");
        File localFile = localFileFactory.apply(file, context);
        File buildLock = acquireBuildLock(localFile);
        try {
            File temporaryFile = temporaryFile(localFile);
            RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
            PersistProcessor<KeyValue> processor = newValueProcessor(serializerFactory.version());

            try (LookupStoreWriter writer =
                            lookupStoreFactory.createWriter(
                                    temporaryFile, bloomFilterFactory.apply(file.rowCount()));
                    RecordReader<KeyValue> reader = readerFactory.createRecordReader(file, context)) {
                writeSst(reader, writer, keySerializer, processor);
            } catch (IOException | RuntimeException e) {
                FileIOUtils.deleteFileOrDirectory(temporaryFile);
                throw e;
            }

            boolean published = false;
            try {
                publish(temporaryFile, localFile);
                published = true;
                LookupStoreReader reader = lookupStoreFactory.createReader(localFile);
                return new ValueSstCacheEntry(
                        file,
                        localFile,
                        keyType,
                        valueType,
                        serializerFactory,
                        serializerFactory.version(),
                        reader);
            } catch (IOException | RuntimeException e) {
                FileIOUtils.deleteFileOrDirectory(temporaryFile);
                if (published) {
                    FileIOUtils.deleteFileOrDirectory(localFile);
                }
                throw e;
            }
        } finally {
            FileIOUtils.deleteFileOrDirectory(buildLock);
        }
    }

    private static File acquireBuildLock(File localFile) throws IOException {
        File parent = localFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Can not create local cache directory: " + parent);
        }
        File buildLock = new File(parent, localFile.getName() + ".lock");
        try {
            Files.createFile(buildLock.toPath());
        } catch (java.nio.file.FileAlreadyExistsException e) {
            throw new IOException("Local cache build is already in progress: " + localFile, e);
        }
        if (localFile.exists()) {
            FileIOUtils.deleteFileOrDirectory(buildLock);
            throw new IOException("Local cache file already exists: " + localFile);
        }
        return buildLock;
    }

    private static File temporaryFile(File localFile) {
        return new File(
                localFile.getParentFile(),
                localFile.getName() + ".building-" + UUID.randomUUID());
    }

    private static void publish(File temporaryFile, File localFile) throws IOException {
        if (localFile.exists()) {
            throw new IOException("Local cache file was published by another builder: " + localFile);
        }
        try {
            Files.move(
                    temporaryFile.toPath(),
                    localFile.toPath(),
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporaryFile.toPath(), localFile.toPath());
        }
    }

    private PersistProcessor<KeyValue> newValueProcessor(String serializerVersion) {
        return PersistValueProcessor.factory(valueType)
                .create(serializerVersion, serializerFactory, null);
    }

    private void writeSst(
            RecordReader<KeyValue> reader,
            LookupStoreWriter writer,
            RowCompactedSerializer keySerializer,
            PersistProcessor<KeyValue> processor)
            throws IOException {
        RecordReader.RecordIterator<KeyValue> batch;
        while ((batch = reader.readBatch()) != null) {
            try {
                KeyValue kv;
                while ((kv = batch.next()) != null) {
                    byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                    byte[] valueBytes = processor.persistToDisk(kv);
                    writer.put(keyBytes, valueBytes);
                }
            } finally {
                batch.releaseBatch();
            }
        }
    }
}
