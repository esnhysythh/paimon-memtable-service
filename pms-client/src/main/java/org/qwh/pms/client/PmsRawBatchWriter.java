package org.qwh.pms.client;

import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.WriteResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PmsRawBatchWriter implements AutoCloseable {

    private final PmsRawClient client;
    private final int maxBatchEntries;
    private final List<RawKvEntry> pending = new ArrayList<>();
    private boolean closed;

    PmsRawBatchWriter(PmsRawClient client, int maxBatchEntries) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        if (maxBatchEntries <= 0) {
            throw new IllegalArgumentException("maxBatchEntries must be positive: " + maxBatchEntries);
        }
        if (maxBatchEntries > client.handshake().maxBatchEntries()) {
            throw new IllegalArgumentException(
                "maxBatchEntries exceeds server limit: "
                    + maxBatchEntries
                    + " > "
                    + client.handshake().maxBatchEntries());
        }
        this.maxBatchEntries = maxBatchEntries;
    }

    public int pendingCount() {
        return pending.size();
    }

    public void put(byte[] key, byte[] row) {
        add(RawKvEntry.put(key, row));
    }

    public void delete(byte[] key) {
        add(RawKvEntry.delete(key));
    }

    public void add(RawKvEntry entry) {
        requireOpen();
        pending.add(Objects.requireNonNull(entry, "entry must not be null"));
        if (pending.size() >= maxBatchEntries) {
            requireOk(flush(), "automatic PMS raw batch flush failed");
        }
    }

    public WriteResult flush() {
        requireOpen();
        if (pending.isEmpty()) {
            return WriteResult.ok(0);
        }
        List<RawKvEntry> entries = List.copyOf(pending);
        WriteResult result = client.writeBatchDetailed(entries);
        if (result.status() == PmsStatus.OK) {
            pending.clear();
        }
        return result;
    }

    @Override
    public void close() {
        if (!closed) {
            requireOk(flush(), "PMS raw batch writer close failed");
            closed = true;
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new PmsClientException("PMS raw batch writer is closed");
        }
    }

    private static void requireOk(WriteResult result, String message) {
        if (result.status() != PmsStatus.OK) {
            throw new PmsWriteException(result.status(), message + ": status=" + result.status());
        }
    }
}
