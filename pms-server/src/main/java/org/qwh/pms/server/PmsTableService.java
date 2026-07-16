package org.qwh.pms.server;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowTypeJson;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.PaimonKeyValueLookupService;
import org.qwh.pms.lookup.cache.LocalCacheDirectory;
import org.qwh.pms.lookup.cache.valuesst.ValueSstCacheBuilder;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouter;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouterOptions;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouterStats;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;
import org.qwh.pms.core.bucket.BucketStateSnapshot;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.bucket.PMSBucketDirector.WriteOp;
import org.qwh.pms.core.bucket.PMSBucketDirectorImpl;
import org.qwh.pms.core.bucket.RecoverySummary;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.config.FlowControlConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.core.storage.SSTState;
import org.qwh.pms.sink.paimon.PaimonCommitPayloadCodec;
import org.qwh.pms.sink.paimon.PaimonSinkManager;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.PmsTableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class PmsTableService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsTableService.class);
    private static final AtomicInteger LOOKUP_BUILD_THREAD_ID = new AtomicInteger();

    private final PaimonTableLoader.LoadedTable loadedTable;
    private final Table table;
    private final FileStoreTable fileStoreTable;
    private final KeyValueFileStore keyValueStore;
    private final PMSBucketDirectorImpl director;
    private final PmsPrimaryKeyCodec keyCodec;
    private final PmsRowValueCodec valueCodec;
    private final JsonRowMapper rowMapper;
    private final List<String> primaryKeys;
    private final int writerSchemaId;
    private final PmsTableSchema protocolTableSchema;
    private final StorageConfig storageConfig;
    private final FlowControlConfig flowControlConfig;
    private final Object paimonCommitPublishLock = new Object();
    /** Serializes each retention check-and-evict loop without blocking Flush or Paimon Sink I/O. */
    private final Object retentionMutex = new Object();
    private final PaimonKeyValueLookupService paimonLookup;
    private final ThresholdFileLookupRouter lookupRouter;
    private final LocalCacheDirectory lookupCacheDirectory;
    private final ExecutorService lookupBuildExecutor;
    private final PaimonCommitPayloadCodec commitPayloadCodec = new PaimonCommitPayloadCodec();
    private final ThreadLocal<RowKeyExtractor> rowKeyExtractors;
    private volatile RuntimeException lookupPayloadDecodeFailure;

    @FunctionalInterface
    interface SinkManagerFactory {
        SinkManager create(
            Table table,
            String commitUser,
            FileLocalStorageManager storageManager
        );
    }

    private PmsTableService(
            PmsServerConfig config,
            PaimonTableLoader.LoadedTable loadedTable,
            PMSBucketDirectorImpl director) {
        this.loadedTable = loadedTable;
        this.table = loadedTable.table();
        this.fileStoreTable = validateLookupProfile(table);
        this.director = director;
        this.storageConfig = config.coreConfig().storage();
        this.flowControlConfig = config.coreConfig().flowcontrol();
        RowType rowType = table.rowType();
        this.primaryKeys = List.copyOf(table.primaryKeys());
        this.writerSchemaId = requireWriterSchemaId(fileStoreTable.schema().id());
        this.keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeys);
        this.valueCodec = new PmsRowValueCodec();
        this.rowMapper = new JsonRowMapper(rowType);
        this.protocolTableSchema = PmsTableSchema.create(
            fileStoreTable.schema().id(),
            PmsTableSchema.ROW_TYPE_FORMAT_PAIMON_JSON_V1,
            PmsRowTypeJson.serialize(rowType),
            primaryKeys,
            fileStoreTable.schema().partitionKeys(),
            PmsRowValueCodec.FORMAT_VERSION,
            PmsPrimaryKeyCodec.FORMAT_VERSION
        );
        this.keyValueStore = (KeyValueFileStore) fileStoreTable.store();
        int[] primaryKeyFieldIndexes = primaryKeyFieldIndexes(rowType, primaryKeys);
        LookupStack lookupStack = createLookupStack(config.lookup(), rowType, primaryKeyFieldIndexes);
        this.paimonLookup = new PaimonKeyValueLookupService(
            new LiveFileIndex(keyValueStore.newKeyComparator(), 4),
            new CandidatePlanner(keyValueStore.newKeyComparator(), 0),
            lookupStack.fileLookup(),
            fileStoreTable.schema().id()
        );
        this.lookupRouter = lookupStack.router();
        this.lookupCacheDirectory = lookupStack.cacheDirectory();
        this.lookupBuildExecutor = lookupStack.buildExecutor();
        this.rowKeyExtractors = ThreadLocal.withInitial(fileStoreTable::createRowKeyExtractor);
    }

    public static PmsTableService open(PmsServerConfig config) throws Exception {
        return open(
            config,
            (table, commitUser, storage) -> new PaimonSinkManager(table, commitUser, storage)
        );
    }

    static PmsTableService open(PmsServerConfig config, SinkManagerFactory sinkManagerFactory) throws Exception {
        LOG.info("Opening PMS table service for {}.{}", config.database(), config.table());
        PaimonTableLoader.LoadedTable loadedTable = new PaimonTableLoader().load(config);
        PMSBucketDirectorImpl director = new PMSBucketDirectorImpl(
            config.coreConfig(),
            storage -> sinkManagerFactory.create(loadedTable.table(), config.commitUser(), storage)
        );
        try {
            director.init();
            LOG.info("PMS table service opened for {}.{}", config.database(), config.table());
            return new PmsTableService(config, loadedTable, director);
        } catch (IOException | RuntimeException e) {
            LOG.error("Failed to open PMS table service for {}.{}", config.database(), config.table(), e);
            director.close();
            loadedTable.close();
            throw e;
        }
    }

    public void write(Map<String, Object> rowValues) {
        InternalRow row = rowMapper.fullRow(rowValues, RowKind.INSERT);
        director.put(keyCodec.encodeKey(row), valueCodec.encode(table.rowType(), row, writerSchemaId));
        LOG.debug("Wrote row to PMS: primaryKeys={}", primaryKeys);
    }

    public void delete(Map<String, Object> primaryKeyValues) {
        director.delete(keyCodec.encodeKeyTuple(rowMapper.keyTuple(primaryKeyValues, primaryKeys)));
        LOG.debug("Deleted row from PMS: primaryKeyValues={}", primaryKeyValues);
    }

    public void writeRawBatch(List<RawKvEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }
        List<WriteOp> ops = entries.stream()
            .map(entry -> entry.isDelete()
                ? WriteOp.delete(entry.key())
                : WriteOp.put(entry.key(), entry.row()))
            .toList();
        director.writeBatch(ops);
        LOG.debug("Wrote raw batch to PMS: recordCount={}", entries.size());
    }

    public boolean isWriteOverloaded() {
        BucketStateSnapshot snapshot = director.stateSnapshot();
        return snapshot.immutableMemTableCount()
                >= flowControlConfig.overloadedImmutableCount()
            || snapshot.newSSTCount()
                >= flowControlConfig.overloadedPendingSstCount();
    }

    public RawLookupResult getLocalRaw(byte[] key) {
        Optional<Value> local = director.lookup(key);
        if (local.isEmpty()) {
            return RawLookupResult.miss();
        }
        Value value = local.get();
        if (value.isTombstone()) {
            return RawLookupResult.deleted();
        }
        return RawLookupResult.hit(value.bytes());
    }

    public RawLookupResult getFullRaw(byte[] key) {
        RawLookupResult local = getLocalRaw(key);
        if (local.type() == LookupResultType.HIT || local.type() == LookupResultType.DELETED) {
            return local;
        }
        GenericRow fullPrimaryKeyRow = rowMapper.fullPrimaryKeyRow((GenericRow) keyCodec.decodeKey(key), primaryKeys);
        return lookupPaimonRaw(fullPrimaryKeyRow);
    }

    public RawLookupBatchResult prefixLocalRaw(byte[] prefix, int maxResults) {
        var entries = director.prefixScan(prefix);
        if (entries.size() > maxResults) {
            return RawLookupBatchResult.failed(PmsStatus.OVERLOADED);
        }
        List<RawLookupResult> rows = entries.stream()
            .map(entry -> RawLookupResult.hit(entry.value().bytes()))
            .toList();
        return RawLookupBatchResult.ok(rows);
    }

    public Optional<Map<String, Object>> get(Map<String, Object> primaryKeyValues) {
        GenericRow fullPrimaryKeyRow = rowMapper.fullPrimaryKeyRow(primaryKeyValues, primaryKeys);
        byte[] key = keyCodec.encodeKey(fullPrimaryKeyRow);
        PmsLocalLookupResult local = getLocal(key);
        if (local.type() == PmsLocalLookupResult.Type.HIT) {
            return Optional.of(local.row());
        }
        if (local.type() == PmsLocalLookupResult.Type.DELETED) {
            return Optional.empty();
        }
        return lookupPaimon(fullPrimaryKeyRow);
    }

    private RawLookupResult lookupPaimonRaw(GenericRow fullPrimaryKeyRow) {
        if (lookupPayloadDecodeFailure != null) {
            throw new PmsLookupUnavailableException(
                "Paimon lookup commit delta publishing failed; restart or repair the lookup view",
                lookupPayloadDecodeFailure
            );
        }
        RowKeyExtractor extractor = rowKeyExtractors.get();
        extractor.setRecord(fullPrimaryKeyRow);
        BinaryRow partition = extractor.partition();
        int bucket = extractor.bucket();
        LookupRequest request = LookupRequest.fullRow(extractor.trimmedPrimaryKey());
        LookupResult result;
        try {
            result = paimonLookup.lookup(partition, bucket, request);
        } catch (IOException e) {
            result = LookupResult.unknown();
        } catch (SchemaMismatchException e) {
            throw new PmsLookupUnavailableException("Paimon lookup schema mismatch", e);
        }
        if (result.kind() == LookupResult.Kind.UNKNOWN) {
            try {
                synchronized (paimonCommitPublishLock) {
                    installSnapshot(partition, bucket);
                    result = paimonLookup.lookup(partition, bucket, request);
                }
            } catch (IOException e) {
                throw new PmsLookupUnavailableException(
                    "Unable to rebuild Paimon lookup view for partition=" + partition + ", bucket=" + bucket,
                    e
                );
            } catch (SchemaMismatchException e) {
                throw new PmsLookupUnavailableException("Paimon lookup schema mismatch", e);
            }
        }
        return switch (result.kind()) {
            case HIT -> RawLookupResult.hit(valueCodec.encode(table.rowType(), result.row().orElseThrow(), writerSchemaId));
            case DELETED -> RawLookupResult.deleted();
            case MISS -> RawLookupResult.miss();
            case UNKNOWN -> throw new PmsLookupUnavailableException(
                "Paimon lookup is unavailable for partition=" + partition + ", bucket=" + bucket);
        };
    }

    public PmsLocalLookupResult getLocal(Map<String, Object> primaryKeyValues) {
        GenericRow keyTuple = rowMapper.keyTuple(primaryKeyValues, primaryKeys);
        return getLocal(keyCodec.encodeKeyTuple(keyTuple));
    }

    public List<Map<String, Object>> prefixLocal(Map<String, Object> primaryKeyPrefixValues) {
        byte[] prefix = keyCodec.encodePrefixTuple(rowMapper.keyPrefixTuple(primaryKeyPrefixValues, primaryKeys));
        List<Map<String, Object>> rows = new ArrayList<>();
        for (var entry : director.prefixScan(prefix)) {
            rows.add(rowMapper.toJsonObject(valueCodec.decode(table.rowType(), entry.value().bytes())));
        }
        return rows;
    }

    public List<Map<String, Object>> prefixScan(Map<String, Object> primaryKeyPrefixValues) {
        throw new PmsNotSupportedException(
            "Full prefix lookup is not supported yet; use prefixLocal for PMS-local prefix lookup"
        );
    }

    public void flush() {
        LOG.info("PMS flush started");
        director.freezeCurMemTable();
        director.flushImmutableMemTable();
        LOG.info("PMS flush completed");
    }

    public void sink() {
        LOG.info("PMS sink started");
        synchronized (paimonCommitPublishLock) {
            director.sinkToPaimon(SinkSelection.allAvailable())
                .commitResult()
                .ifPresent(this::publishCommittedLookupDelta);
        }
        retainLocalSSTsUntilWithinLimits();
        LOG.info("PMS sink completed");
    }

    public void retainLocalSSTs() {
        retainLocalSSTsUntilWithinLimits();
    }

    public Map<String, Object> state() {
        Map<String, Object> state = stateToMap(director.stateSnapshot());
        appendLookupState(state);
        return state;
    }

    public Map<String, Object> recoverySummary() {
        return recoverySummaryToMap(director.lastRecoverySummary());
    }

    public PmsTableSchema protocolTableSchema() {
        return protocolTableSchema;
    }

    private Optional<Map<String, Object>> lookupPaimon(GenericRow fullPrimaryKeyRow) {
        if (lookupPayloadDecodeFailure != null) {
            throw new PmsLookupUnavailableException(
                "Paimon lookup commit delta publishing failed; restart or repair the lookup view",
                lookupPayloadDecodeFailure
            );
        }
        RowKeyExtractor extractor = rowKeyExtractors.get();
        extractor.setRecord(fullPrimaryKeyRow);
        BinaryRow partition = extractor.partition();
        int bucket = extractor.bucket();
        LookupRequest request = LookupRequest.fullRow(extractor.trimmedPrimaryKey());
        LookupResult result;
        try {
            result = paimonLookup.lookup(partition, bucket, request);
        } catch (IOException e) {
            result = LookupResult.unknown();
        } catch (SchemaMismatchException e) {
            throw new PmsLookupUnavailableException("Paimon lookup schema mismatch", e);
        }
        if (result.kind() == LookupResult.Kind.UNKNOWN) {
            try {
                synchronized (paimonCommitPublishLock) {
                    installSnapshot(partition, bucket);
                    result = paimonLookup.lookup(partition, bucket, request);
                }
            } catch (IOException e) {
                throw new PmsLookupUnavailableException(
                    "Unable to rebuild Paimon lookup view for partition=" + partition + ", bucket=" + bucket,
                    e
                );
            } catch (SchemaMismatchException e) {
                throw new PmsLookupUnavailableException("Paimon lookup schema mismatch", e);
            }
        }
        return switch (result.kind()) {
            case HIT -> Optional.of(rowMapper.toJsonObject(result.row().orElseThrow()));
            case DELETED, MISS -> Optional.empty();
            case UNKNOWN -> throw new PmsLookupUnavailableException(
                "Paimon lookup is unavailable for partition=" + partition + ", bucket=" + bucket);
        };
    }

    private PmsLocalLookupResult getLocal(byte[] key) {
        Optional<Value> local = director.lookup(key);
        if (local.isEmpty()) {
            return PmsLocalLookupResult.miss();
        }
        Value value = local.get();
        if (value.isTombstone()) {
            return PmsLocalLookupResult.deleted();
        }
        return PmsLocalLookupResult.hit(rowMapper.toJsonObject(valueCodec.decode(table.rowType(), value.bytes())));
    }

    private void installSnapshot(BinaryRow partition, int bucket) throws IOException {
        long startedNanos = System.nanoTime();
        List<org.apache.paimon.io.DataFileMeta> files = fileStoreTable.store().newScan()
            .withPartitionBucket(partition, bucket)
            .plan()
            .files(FileKind.ADD)
            .stream()
            .map(entry -> entry.file())
            .toList();
        paimonLookup.installSnapshot(partition, bucket, files);
        LOG.info(
            "Installed Paimon lookup snapshot: partition={}, bucket={}, liveFileCount={}, durationMs={}",
            partition,
            bucket,
            files.size(),
            elapsedMillis(startedNanos)
        );
    }

    private void publishCommittedLookupDelta(SinkCommitResult result) {
        byte[] payload = result.commitPayload();
        if (payload.length == 0) {
            return;
        }
        List<org.apache.paimon.table.sink.CommitMessage> messages;
        try {
            messages = commitPayloadCodec.decode(payload);
        } catch (RuntimeException e) {
            lookupPayloadDecodeFailure = e;
            throw new PmsLookupUnavailableException(
                "Paimon commit succeeded but its lookup delta could not be published", e);
        }
        try {
            synchronized (paimonCommitPublishLock) {
                paimonLookup.applyCommittedMessages(messages);
            }
        } catch (RuntimeException e) {
            throw new PmsLookupUnavailableException(
                "Paimon commit succeeded but its lookup delta could not be applied", e);
        }
    }

    private static FileStoreTable validateLookupProfile(Table table) {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new IllegalArgumentException("Paimon lookup requires a FileStoreTable");
        }
        if (fileStoreTable.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException("Paimon lookup requires a primary-key table");
        }
        if (fileStoreTable.bucketMode() != BucketMode.HASH_FIXED) {
            throw new IllegalArgumentException(
                "Paimon lookup requires HASH_FIXED buckets, actual=" + fileStoreTable.bucketMode());
        }
        CoreOptions options = new CoreOptions(fileStoreTable.options());
        if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new IllegalArgumentException(
                "Paimon lookup requires merge-engine=deduplicate, actual=" + options.mergeEngine());
        }
        if (!CoreOptions.FILE_FORMAT_PARQUET.equals(options.fileFormatString())) {
            throw new IllegalArgumentException(
                "Paimon lookup requires parquet data files, actual=" + options.fileFormatString());
        }
        return fileStoreTable;
    }

    private static int[] primaryKeyFieldIndexes(RowType rowType, List<String> primaryKeys) {
        return primaryKeys.stream()
            .map(rowType::getField)
            .mapToInt(field -> rowType.getFieldIndexByFieldId(field.id()))
            .toArray();
    }

    private static int requireWriterSchemaId(long schemaId) {
        if (schemaId < 0 || schemaId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Paimon schema id is outside PMS row codec range: " + schemaId);
        }
        return (int) schemaId;
    }

    private LookupStack createLookupStack(
            PmsLookupConfig lookupConfig, RowType rowType, int[] primaryKeyFieldIndexes) {
        DataFileLookup directLookup = new PaimonKeyValueParquetLookup(
            rowType,
            primaryKeyFieldIndexes,
            fileStoreTable.schema().id(),
            this::resolveDataFile,
            new Options(),
            1024,
            lookupConfig.directMetadataCacheEntries()
        );
        if (!lookupConfig.cacheEnabled()) {
            LOG.info("Paimon lookup ValueSST cache disabled; using direct Parquet lookup only");
            return new LookupStack(directLookup, null, null, null);
        }

        LocalCacheDirectory cacheDirectory = null;
        ExecutorService buildExecutor = null;
        try {
            cacheDirectory = new LocalCacheDirectory(lookupConfig.cacheDir());
            buildExecutor = Executors.newFixedThreadPool(
                lookupConfig.buildThreads(),
                lookupBuildThreadFactory()
            );
            ThresholdFileLookupRouter router = new ThresholdFileLookupRouter(
                directLookup,
                valueSstCacheBuilder(rowType, primaryKeyFieldIndexes, cacheDirectory),
                buildExecutor,
                new ThresholdFileLookupRouterOptions(
                    lookupConfig.buildThreshold(),
                    lookupConfig.buildThreads(),
                    lookupConfig.maxCacheBytes(),
                    lookupConfig.buildTimeout(),
                    lookupConfig.retryBackoff()
                )
            );
            LOG.info(
                "Paimon lookup ValueSST cache enabled: dir={}, maxBytes={}, buildThreshold={}, buildThreads={}, buildTimeout={}, retryBackoff={}",
                lookupConfig.cacheDir(),
                lookupConfig.maxCacheBytes(),
                lookupConfig.buildThreshold(),
                lookupConfig.buildThreads(),
                lookupConfig.buildTimeout(),
                lookupConfig.retryBackoff()
            );
            return new LookupStack(router, router, cacheDirectory, buildExecutor);
        } catch (IOException | RuntimeException e) {
            if (buildExecutor != null) {
                buildExecutor.shutdownNow();
            }
            if (cacheDirectory != null) {
                try {
                    cacheDirectory.close();
                } catch (IOException closeError) {
                    e.addSuppressed(closeError);
                }
            }
            throw new IllegalStateException(
                "Failed to initialize Paimon lookup ValueSST cache at " + lookupConfig.cacheDir(),
                e
            );
        }
    }

    private ValueSstCacheBuilder valueSstCacheBuilder(
            RowType rowType, int[] primaryKeyFieldIndexes, LocalCacheDirectory cacheDirectory) {
        CoreOptions options = new CoreOptions(fileStoreTable.options());
        RowType fileKeyType = PrimaryKeyTableUtils.addKeyNamePrefix(rowType.project(primaryKeyFieldIndexes));
        LookupStoreFactory lookupStoreFactory = LookupStoreFactory.create(
            options,
            new CacheManager(options.lookupCacheMaxMemory(), options.lookupCacheHighPrioPoolRatio()),
            new RowCompactedSerializer(fileKeyType).createSliceComparator()
        );
        return new ValueSstCacheBuilder(
            fileKeyType,
            rowType,
            (file, context) -> {
                KeyValueFileReaderFactory readerFactory = keyValueStore
                    .newReaderFactoryBuilder()
                    .build(context.partition(), context.bucket(), DeletionVector.emptyFactory());
                return readerFactory.createRecordReader(file);
            },
            lookupStoreFactory,
            LookupSerializerFactory.INSTANCE.get(),
            LookupStoreFactory.bfGenerator(options.toConfiguration()),
            cacheDirectory
        );
    }

    private ResolvedDataFile resolveDataFile(FileLookupContext context, org.apache.paimon.io.DataFileMeta file)
            throws IOException {
        DataFilePathFactory pathFactory = fileStoreTable.store().pathFactory()
            .createDataFilePathFactory(context.partition(), context.bucket());
        org.apache.paimon.fs.Path path = pathFactory.toPath(file);
        return new ResolvedDataFile(table.fileIO(), path, table.fileIO().getFileStatus(path).getLen());
    }

    private void appendLookupState(Map<String, Object> state) {
        state.put("lookupCacheEnabled", lookupRouter != null);
        if (lookupRouter == null) {
            return;
        }
        ThresholdFileLookupRouterStats stats = lookupRouter.stats();
        state.put("lookupDirectLookups", stats.directLookups());
        state.put("lookupLocalLookups", stats.localLookups());
        state.put("lookupCacheBuildsScheduled", stats.buildsScheduled());
        state.put("lookupCacheBuildsSucceeded", stats.buildsSucceeded());
        state.put("lookupCacheBuildsFailed", stats.buildsFailed());
        state.put("lookupCacheBuildsTimedOut", stats.buildsTimedOut());
        state.put("lookupCacheBuildsRejected", stats.buildsRejected());
        state.put("lookupCacheEntriesEvicted", stats.entriesEvicted());
        state.put("lookupCacheReadyEntries", stats.readyEntries());
        state.put("lookupCacheBytes", stats.cacheBytes());
        state.put("lookupCacheInFlightBuilds", stats.inFlightBuilds());
    }

    private static ThreadFactory lookupBuildThreadFactory() {
        return runnable -> {
            Thread thread = new Thread(
                runnable,
                "pms-lookup-cache-build-" + LOOKUP_BUILD_THREAD_ID.incrementAndGet()
            );
            thread.setDaemon(true);
            return thread;
        };
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    private void retainLocalSSTsUntilWithinLimits() {
        synchronized (retentionMutex) {
            int evictedCount = 0;
            while (true) {
                BucketStateSnapshot state = director.stateSnapshot();
                if (!shouldEvictSinkedSST(state)) {
                    break;
                }
                if (state.sinkedSSTCount() > storageConfig.sinkedMaxCount()) {
                    Optional<CompactionSelection> compaction = selectSinkedCompaction(state);
                    if (compaction.isPresent()) {
                        director.compactLocalSSTs(compaction.get());
                        state = director.stateSnapshot();
                        if (!shouldEvictSinkedSST(state)) {
                            continue;
                        }
                    }
                }
                var evicted = director.evictOldestSinkedSST().evictedRun();
                if (evicted.isEmpty()) {
                    break;
                }
                evictedCount++;
                LOG.info(
                    "Evicted sinked SST by retention: runId={}, fileSize={}, entryCount={}",
                    evicted.get().runId(),
                    evicted.get().fileSizeBytes(),
                    evicted.get().entryCount()
                );
            }
            if (evictedCount > 0) {
                LOG.info("PMS local SST retention completed, evictedSinkedSSTCount={}", evictedCount);
            }
        }
    }

    private Optional<CompactionSelection> selectSinkedCompaction(BucketStateSnapshot state) {
        List<LocalRunSnapshot> sinkedRuns = state.localRuns().stream()
            .filter(run -> run.state() == SSTState.SINKED)
            .toList();
        if (sinkedRuns.size() < storageConfig.compactMinFiles()) {
            return Optional.empty();
        }
        long maxInputBytes = storageConfig.compactThresholdMb() * 1024L * 1024L;
        List<Long> selectedRunIds = new ArrayList<>();
        long selectedBytes = 0;
        long previousMaxFlushId = -1;
        for (LocalRunSnapshot run : sinkedRuns) {
            boolean continuous = selectedRunIds.isEmpty()
                || previousMaxFlushId + 1 == run.minFlushId();
            boolean fits = run.fileSizeBytes() <= maxInputBytes
                && run.fileSizeBytes() <= maxInputBytes - selectedBytes;
            if (!continuous || !fits) {
                if (selectedRunIds.size() >= storageConfig.compactMinFiles()) {
                    break;
                }
                selectedRunIds.clear();
                selectedBytes = 0;
                previousMaxFlushId = -1;
            }
            if (run.fileSizeBytes() <= maxInputBytes) {
                selectedRunIds.add(run.runId());
                selectedBytes += run.fileSizeBytes();
                previousMaxFlushId = run.maxFlushId();
            }
        }
        return selectedRunIds.size() >= storageConfig.compactMinFiles()
            ? Optional.of(new CompactionSelection(SSTState.SINKED, selectedRunIds))
            : Optional.empty();
    }

    private boolean shouldEvictSinkedSST(BucketStateSnapshot state) {
        if (state.sinkedSSTCount() <= 0) {
            return false;
        }
        long localSSTCount = (long) state.newSSTCount() + state.sinkedSSTCount();
        long localSSTBytes = state.newSSTTotalBytes() + state.sinkedSSTTotalBytes();
        long localSSTRows = state.newSSTTotalRows() + state.sinkedSSTTotalRows();
        long maxBytes = storageConfig.sinkedMaxSizeMb() * 1024L * 1024L;
        return localSSTBytes > maxBytes
            || localSSTCount > storageConfig.sinkedMaxCount()
            || (storageConfig.localSstMaxRows() > 0 && localSSTRows > storageConfig.localSstMaxRows());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing PMS table service");
        Exception failure = null;
        try {
            if (lookupRouter != null) {
                lookupRouter.close();
            }
        } catch (Exception e) {
            failure = e;
        }
        shutdownLookupBuildExecutor();
        try {
            if (lookupCacheDirectory != null) {
                lookupCacheDirectory.close();
            }
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            director.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            loadedTable.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        LOG.info("PMS table service closed");
        if (failure != null) {
            throw failure;
        }
    }

    private void shutdownLookupBuildExecutor() {
        if (lookupBuildExecutor == null) {
            return;
        }
        lookupBuildExecutor.shutdownNow();
        try {
            if (!lookupBuildExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Paimon lookup cache build executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while stopping Paimon lookup cache build executor", e);
        }
    }

    private static Map<String, Object> stateToMap(BucketStateSnapshot state) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("observedAtMillis", state.observedAtMillis());
        result.put("curMemTableEstimatedEntryCount", state.curMemTableEstimatedEntryCount());
        result.put("curMemTableSizeBytes", state.curMemTableSizeBytes());
        result.put("curMemTableMinSequenceId", state.curMemTableMinSequenceId());
        result.put("curMemTableMaxSequenceId", state.curMemTableMaxSequenceId());
        result.put("curMemTableOldestWriteAtMillis", state.curMemTableOldestWriteAtMillis());
        result.put("curMemTableAgeMillis", state.curMemTableAgeMillis());
        result.put("immutableMemTableCount", state.immutableMemTableCount());
        result.put("immutableMemTableTotalBytes", state.immutableMemTableTotalBytes());
        result.put("immutableMemTableMinSequenceId", state.immutableMemTableMinSequenceId());
        result.put("immutableMemTableMaxSequenceId", state.immutableMemTableMaxSequenceId());
        result.put("immutableMemTableOldestWriteAtMillis", state.immutableMemTableOldestWriteAtMillis());
        result.put("immutableMemTableAgeMillis", state.immutableMemTableAgeMillis());
        result.put("lastAssignedSequenceId", state.lastAssignedSequenceId());
        result.put("lastFlushedSequenceId", state.lastFlushedSequenceId());
        result.put("lastPersistedSequenceId", state.lastPersistedSequenceId());
        result.put("newSSTCount", state.newSSTCount());
        result.put("newSSTTotalBytes", state.newSSTTotalBytes());
        result.put("newSSTTotalRows", state.newSSTTotalRows());
        result.put("newSSTMinSequenceId", state.newSSTMinSequenceId());
        result.put("newSSTMaxSequenceId", state.newSSTMaxSequenceId());
        result.put("newSSTOldestWriteAtMillis", state.newSSTOldestWriteAtMillis());
        result.put("newSSTAgeMillis", state.newSSTAgeMillis());
        result.put("sinkedSSTCount", state.sinkedSSTCount());
        result.put("sinkedSSTTotalBytes", state.sinkedSSTTotalBytes());
        result.put("sinkedSSTTotalRows", state.sinkedSSTTotalRows());
        result.put("sinkedSSTMinSequenceId", state.sinkedSSTMinSequenceId());
        result.put("sinkedSSTMaxSequenceId", state.sinkedSSTMaxSequenceId());
        result.put("sinkedSSTOldestWriteAtMillis", state.sinkedSSTOldestWriteAtMillis());
        result.put("sinkedSSTAgeMillis", state.sinkedSSTAgeMillis());
        result.put("localRuns", state.localRuns());
        result.put("sinkFlight", state.sinkFlight());
        result.put("recoveredUnpersistedData", state.recoveredUnpersistedData());
        result.put("lastSinkedSnapshotId", state.lastSinkedSnapshotId());
        return result;
    }

    private static Map<String, Object> recoverySummaryToMap(RecoverySummary recovery) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("recoveredDataRecords", recovery.recoveredDataRecords());
        result.put("skippedFlushedRecords", recovery.skippedFlushedRecords());
        result.put("lastFlushedSequenceId", recovery.lastFlushedSequenceId());
        result.put("pendingPreparedSinkCount", recovery.pendingPreparedSinkCount());
        result.put("recoveredPreparedSinkCount", recovery.recoveredPreparedSinkCount());
        result.put("recoveredSinkedSSTCount", recovery.recoveredSinkedSSTCount());
        result.put("lastSinkedSnapshotId", recovery.lastSinkedSnapshotId());
        result.put("newSSTCount", recovery.newSSTCount());
        result.put("sinkedSSTCount", recovery.sinkedSSTCount());
        result.put("curMemTableEstimatedEntryCount", recovery.curMemTableEstimatedEntryCount());
        return result;
    }

    private record LookupStack(
            DataFileLookup fileLookup,
            ThresholdFileLookupRouter router,
            LocalCacheDirectory cacheDirectory,
            ExecutorService buildExecutor) {}

}
