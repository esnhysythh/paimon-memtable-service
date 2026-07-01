package org.qwh.pms.client;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.WriteResult;

import java.net.URI;
import java.util.List;
import java.util.Objects;

public final class PmsClient implements AutoCloseable {

    private final PmsRawClient rawClient;
    private final RowType rowType;
    private final List<String> primaryKeyFieldNames;
    private final int writerSchemaId;
    private final PmsPrimaryKeyCodec keyCodec;
    private final PmsRowValueCodec valueCodec;

    public static PmsClient connect(
            PmsClientConfig config,
            RowType rowType,
            List<String> primaryKeyFieldNames) {
        return connect(config, rowType, primaryKeyFieldNames, 0);
    }

    public static PmsClient connect(
            PmsClientConfig config,
            RowType rowType,
            List<String> primaryKeyFieldNames,
            int writerSchemaId) {
        return new PmsClient(PmsRawClient.connect(config), rowType, primaryKeyFieldNames, writerSchemaId);
    }

    public static PmsClient connect(
            URI serverUri,
            RowType rowType,
            List<String> primaryKeyFieldNames) {
        return connect(PmsClientConfig.forUri(serverUri), rowType, primaryKeyFieldNames);
    }

    PmsClient(
            PmsRawClient rawClient,
            RowType rowType,
            List<String> primaryKeyFieldNames,
            int writerSchemaId) {
        this.rawClient = Objects.requireNonNull(rawClient, "rawClient must not be null");
        this.rowType = Objects.requireNonNull(rowType, "rowType must not be null");
        Objects.requireNonNull(primaryKeyFieldNames, "primaryKeyFieldNames must not be null");
        if (primaryKeyFieldNames.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyFieldNames must not be empty");
        }
        if (writerSchemaId < 0) {
            throw new IllegalArgumentException("writerSchemaId must not be negative: " + writerSchemaId);
        }
        this.primaryKeyFieldNames = List.copyOf(primaryKeyFieldNames);
        this.writerSchemaId = writerSchemaId;
        this.keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, this.primaryKeyFieldNames);
        this.valueCodec = new PmsRowValueCodec();
    }

    public PmsHandshake handshake() {
        return rawClient.handshake();
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> primaryKeyFieldNames() {
        return primaryKeyFieldNames;
    }

    public int writerSchemaId() {
        return writerSchemaId;
    }

    public PmsRawClient rawClient() {
        return rawClient;
    }

    public WriteResult write(InternalRow row) {
        return rawClient.writeBatchDetailed(List.of(toRawEntry(row)));
    }

    public WriteResult writeBatch(List<? extends InternalRow> rows) {
        Objects.requireNonNull(rows, "rows must not be null");
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("rows must not be empty");
        }
        List<RawKvEntry> entries = rows.stream()
            .map(this::toRawEntry)
            .toList();
        return rawClient.writeBatchDetailed(entries);
    }

    public WriteResult delete(InternalRow keyTuple) {
        return rawClient.deleteDetailed(keyCodec.encodeKeyTuple(keyTuple));
    }

    public PmsRowLookupResult get(InternalRow keyTuple) {
        return PmsRowLookupResult.fromRaw(rawClient.getFull(keyCodec.encodeKeyTuple(keyTuple)), valueCodec, rowType);
    }

    public PmsRowLookupResult getLocal(InternalRow keyTuple) {
        return PmsRowLookupResult.fromRaw(rawClient.getLocal(keyCodec.encodeKeyTuple(keyTuple)), valueCodec, rowType);
    }

    public PmsRowLookupBatchResult prefixLocal(InternalRow keyPrefixTuple) {
        RawLookupBatchResult result = rawClient.getPrefixLocal(keyCodec.encodePrefixTuple(keyPrefixTuple));
        return PmsRowLookupBatchResult.fromRaw(result, valueCodec, rowType);
    }

    public byte[] encodeKey(InternalRow fullRow) {
        return keyCodec.encodeKey(fullRow);
    }

    public byte[] encodeKeyTuple(InternalRow keyTuple) {
        return keyCodec.encodeKeyTuple(keyTuple);
    }

    public byte[] encodePrefixTuple(InternalRow keyPrefixTuple) {
        return keyCodec.encodePrefixTuple(keyPrefixTuple);
    }

    public byte[] encodeRowValue(InternalRow row) {
        return valueCodec.encode(rowType, row, writerSchemaId);
    }

    @Override
    public void close() {
        rawClient.close();
    }

    private RawKvEntry toRawEntry(InternalRow row) {
        Objects.requireNonNull(row, "row must not be null");
        byte[] key = keyCodec.encodeKey(row);
        RowKind rowKind = row.getRowKind();
        return switch (rowKind) {
            case INSERT, UPDATE_AFTER -> RawKvEntry.put(key, valueCodec.encode(rowType, row, writerSchemaId));
            case DELETE, UPDATE_BEFORE -> RawKvEntry.delete(key);
        };
    }
}
