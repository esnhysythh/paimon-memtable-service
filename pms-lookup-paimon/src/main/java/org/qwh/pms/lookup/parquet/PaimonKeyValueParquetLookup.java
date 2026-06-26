package org.qwh.pms.lookup.parquet;

import org.apache.paimon.KeyValue;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.DataFileResolver;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupRequestValidator;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.LookupUnknownException;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.parquet.key.KeyTypeCodecs;
import org.qwh.pms.lookup.parquet.key.LookupKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/** Direct Parquet lookup path for real Paimon primary-key KeyValue data files. */
public class PaimonKeyValueParquetLookup implements DataFileLookup {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonKeyValueParquetLookup.class);

    private final RowType keyType;
    private final long expectedSchemaId;
    private final LookupKeySpec physicalKeySpec;
    private final DataFileResolver fileResolver;
    private final ParquetKeyRowLocator keyRowLocator;
    private final PaimonKeyValueRowReader rowReader;
    private final PaimonValueProjector valueProjector;
    private final FilterPredicateRowRangeProvider rowRangeProvider;
    private final ParquetLookupMetadataCache metadataCache;

    public PaimonKeyValueParquetLookup(
            RowType valueType,
            int[] keyFieldIndexes,
            long expectedSchemaId,
            DataFileResolver fileResolver) {
        this(valueType, keyFieldIndexes, expectedSchemaId, fileResolver, new Options(), 1024);
    }

    public PaimonKeyValueParquetLookup(
            RowType valueType,
            int[] keyFieldIndexes,
            long expectedSchemaId,
            DataFileResolver fileResolver,
            Options options,
            int batchSize) {
        this(
                valueType,
                keyFieldIndexes,
                expectedSchemaId,
                fileResolver,
                options,
                batchSize,
                ParquetLookupMetadataCache.DEFAULT_MAX_ENTRIES);
    }

    public PaimonKeyValueParquetLookup(
            RowType valueType,
            int[] keyFieldIndexes,
            long expectedSchemaId,
            DataFileResolver fileResolver,
            Options options,
            int batchSize,
            int metadataCacheMaxEntries) {
        this.keyType = PrimaryKeyTableUtils.addKeyNamePrefix(valueType.project(keyFieldIndexes));
        this.expectedSchemaId = expectedSchemaId;
        RowType physicalRowType = KeyValue.schema(keyType, valueType);
        this.physicalKeySpec = LookupKeySpec.of(physicalRowType, physicalKeyIndexes(keyType));
        this.fileResolver = fileResolver;
        this.keyRowLocator = new ParquetKeyRowLocator(physicalKeySpec, options, batchSize);
        this.rowReader =
                new PaimonKeyValueRowReader(
                        keyType, valueType, physicalRowType, options, batchSize);
        this.valueProjector = new PaimonValueProjector(valueType);
        this.rowRangeProvider = new FilterPredicateRowRangeProvider(physicalKeySpec, options);
        this.metadataCache = new ParquetLookupMetadataCache(options, metadataCacheMaxEntries);
        ensureSupportedKeyType();
    }

    @Override
    public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request)
            throws IOException {
        ensureRequestKeyMatchesSpec(request);
        ensureSchemaMatches(file);
        try {
            ResolvedDataFile resolved = fileResolver.resolve(context, file);
            ResolvedDataFileKey cacheKey = ResolvedDataFileKey.of(file, resolved);
            ParquetLookupMetadata metadata = metadataCache.getOrLoad(cacheKey, resolved);

            for (RowRangeCandidate rowRange :
                    rowRangeProvider.candidateRowRanges(metadata, request.key())) {
                long rowIndex = keyRowLocator.locate(resolved, rowRange, request.key());
                if (rowIndex >= 0) {
                    KeyValue keyValue = rowReader.read(resolved, rowIndex);
                    if (keyValue.valueKind().isRetract()) {
                        return LookupResult.deleted(rowIndex);
                    }
                    return LookupResult.hit(valueProjector.copy(keyValue.value(), request), rowIndex);
                }
            }
            return LookupResult.miss();
        } catch (LookupUnknownException | IOException e) {
            LOG.warn(
                    "Direct Paimon Parquet lookup failed: partition={}, bucket={}, file={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName(),
                    e);
            return LookupResult.unknown();
        }
    }

    @Override
    public void invalidate(FileLookupContext context, DataFileMeta file) {
        metadataCache.invalidate(file.fileName());
    }

    @Override
    public void invalidateBucket(FileLookupContext context) {
        // This direct lookup instance is table-scoped, while metadata keys are not bucket-indexed.
        metadataCache.clear();
    }

    public void invalidate(String fileName) {
        metadataCache.invalidate(fileName);
    }

    private void ensureSupportedKeyType() {
        for (DataType type : keyType.getFieldTypes()) {
            if (!KeyTypeCodecs.isSupported(type)) {
                throw new UnsupportedOperationException(
                        "Direct Paimon KeyValue Parquet lookup does not support key type: "
                                + keyType);
            }
        }
    }

    private void ensureRequestKeyMatchesSpec(LookupRequest request) {
        LookupRequestValidator.ensureKeyFieldCount(request, keyType.getFieldCount(), "key type");
        LookupRequestValidator.ensureNonNullKey(request);
    }

    private void ensureSchemaMatches(DataFileMeta file) {
        if (file.schemaId() != expectedSchemaId) {
            throw new SchemaMismatchException(file.fileName(), expectedSchemaId, file.schemaId());
        }
    }

    private static int[] physicalKeyIndexes(RowType keyType) {
        int[] indexes = new int[keyType.getFieldCount()];
        Arrays.setAll(indexes, i -> i);
        return indexes;
    }
}
