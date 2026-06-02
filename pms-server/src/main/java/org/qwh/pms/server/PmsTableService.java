package org.qwh.pms.server;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.bucket.BucketStateSnapshot;
import org.qwh.pms.core.bucket.PMSBucketDirectorImpl;
import org.qwh.pms.core.bucket.RecoverySummary;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.sink.paimon.PaimonSinkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public final class PmsTableService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsTableService.class);

    private final PaimonTableLoader.LoadedTable loadedTable;
    private final Table table;
    private final PMSBucketDirectorImpl director;
    private final PmsPrimaryKeyCodec keyCodec;
    private final PmsRowValueCodec valueCodec;
    private final JsonRowMapper rowMapper;
    private final List<String> primaryKeys;
    private final StorageConfig storageConfig;
    private final ReentrantLock maintenanceLock = new ReentrantLock();

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
        this.director = director;
        this.storageConfig = config.coreConfig().storage();
        RowType rowType = table.rowType();
        this.primaryKeys = List.copyOf(table.primaryKeys());
        this.keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeys);
        this.valueCodec = new PmsRowValueCodec();
        this.rowMapper = new JsonRowMapper(rowType);
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
        director.put(keyCodec.encodeKey(row), valueCodec.encode(table.rowType(), row, 0));
        LOG.debug("Wrote row to PMS: primaryKeys={}", primaryKeys);
    }

    public void delete(Map<String, Object> primaryKeyValues) {
        director.delete(keyCodec.encodeKeyTuple(rowMapper.keyTuple(primaryKeyValues, primaryKeys)));
        LOG.debug("Deleted row from PMS: primaryKeyValues={}", primaryKeyValues);
    }

    public Optional<Map<String, Object>> get(Map<String, Object> primaryKeyValues) {
        GenericRow keyTuple = rowMapper.keyTuple(primaryKeyValues, primaryKeys);
        byte[] key = keyCodec.encodeKeyTuple(keyTuple);
        PmsLocalLookupResult local = getLocal(key);
        if (local.type() == PmsLocalLookupResult.Type.HIT) {
            return Optional.of(local.row());
        }
        if (local.type() == PmsLocalLookupResult.Type.DELETED) {
            return Optional.empty();
        }
        return lookupPaimon(keyTuple);
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
        maintenanceLock.lock();
        try {
            LOG.info("PMS flush started");
            director.freezeCurMemTable();
            director.flushImmutableMemTable();
            LOG.info("PMS flush completed");
        } finally {
            maintenanceLock.unlock();
        }
    }

    public void sink() {
        maintenanceLock.lock();
        try {
            LOG.info("PMS sink started");
            director.sinkToPaimon();
            retainLocalSSTsLocked();
            LOG.info("PMS sink completed");
        } finally {
            maintenanceLock.unlock();
        }
    }

    public void retainLocalSSTs() {
        maintenanceLock.lock();
        try {
            retainLocalSSTsLocked();
        } finally {
            maintenanceLock.unlock();
        }
    }

    public Map<String, Object> state() {
        return stateToMap(director.stateSnapshot());
    }

    public Map<String, Object> recoverySummary() {
        return recoverySummaryToMap(director.lastRecoverySummary());
    }

    private Optional<Map<String, Object>> lookupPaimon(GenericRow keyTuple) {
        ReadBuilder readBuilder = table.newReadBuilder()
            .withFilter(primaryKeyPredicate(keyTuple))
            .withLimit(1);
        try (RecordReader<InternalRow> reader =
                 readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            PaimonLookupResult result = new PaimonLookupResult();
            reader.forEachRemaining(row -> {
                if (result.row == null) {
                    result.row = rowMapper.toJsonObject(row);
                }
            });
            return Optional.ofNullable(result.row);
        } catch (IOException e) {
            throw new RuntimeException("Paimon point lookup failed for primary keys " + primaryKeys, e);
        }
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

    private Predicate primaryKeyPredicate(GenericRow keyTuple) {
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        List<Predicate> predicates = new ArrayList<>(primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++) {
            predicates.add(builder.equal(builder.indexOf(primaryKeys.get(i)), keyTuple.getField(i)));
        }
        return PredicateBuilder.and(predicates);
    }

    private void retainLocalSSTsLocked() {
        int evictedCount = 0;
        while (shouldEvictSinkedSST(director.stateSnapshot())) {
            var evicted = director.evictOldestSinkedSST();
            if (evicted.isEmpty()) {
                break;
            }
            evictedCount++;
            LOG.info(
                "Evicted sinked SST by retention: runId={}, fileSize={}, entryCount={}",
                evicted.get().runId(),
                evicted.get().fileSize(),
                evicted.get().entryCount()
            );
        }
        if (evictedCount > 0) {
            LOG.info("PMS local SST retention completed, evictedSinkedSSTCount={}", evictedCount);
        }
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
        director.close();
        loadedTable.close();
        LOG.info("PMS table service closed");
    }

    private static Map<String, Object> stateToMap(BucketStateSnapshot state) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("curMemTableEstimatedEntryCount", state.curMemTableEstimatedEntryCount());
        result.put("curMemTableSizeBytes", state.curMemTableSizeBytes());
        result.put("immutableMemTableCount", state.immutableMemTableCount());
        result.put("immutableMemTableTotalBytes", state.immutableMemTableTotalBytes());
        result.put("lastAssignedSequenceId", state.lastAssignedSequenceId());
        result.put("curMemTableMinSequenceId", state.curMemTableMinSequenceId());
        result.put("curMemTableMaxSequenceId", state.curMemTableMaxSequenceId());
        result.put("immutableMemTableMinSequenceId", state.immutableMemTableMinSequenceId());
        result.put("immutableMemTableMaxSequenceId", state.immutableMemTableMaxSequenceId());
        result.put("lastFlushedSequenceId", state.lastFlushedSequenceId());
        result.put("newSSTCount", state.newSSTCount());
        result.put("newSSTTotalBytes", state.newSSTTotalBytes());
        result.put("newSSTTotalRows", state.newSSTTotalRows());
        result.put("newSSTMinSequenceId", state.newSSTMinSequenceId());
        result.put("newSSTMaxSequenceId", state.newSSTMaxSequenceId());
        result.put("sinkedSSTCount", state.sinkedSSTCount());
        result.put("sinkedSSTTotalBytes", state.sinkedSSTTotalBytes());
        result.put("sinkedSSTTotalRows", state.sinkedSSTTotalRows());
        result.put("withMemCount", state.withMemCount());
        result.put("withMemTotalBytes", state.withMemTotalBytes());
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

    private static final class PaimonLookupResult {
        private Map<String, Object> row;
    }
}
