package org.qwh.pms.server;

import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawKvStore;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;

import java.util.List;
import java.util.Objects;

final class PmsRuntimeRawKvStore implements RawKvStore {

    private final PmsServerRuntime runtime;

    PmsRuntimeRawKvStore(PmsServerRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null");
    }

    @Override
    public PmsStatus put(byte[] key, byte[] row) {
        return writeBatch(List.of(RawKvEntry.put(key, row)));
    }

    @Override
    public PmsStatus delete(byte[] key) {
        return writeBatch(List.of(RawKvEntry.delete(key)));
    }

    @Override
    public PmsStatus writeBatch(List<RawKvEntry> entries) {
        try {
            runtime.writeRawBatch(entries);
            return PmsStatus.OK;
        } catch (PmsOverloadedException e) {
            return PmsStatus.OVERLOADED;
        } catch (PmsServiceUnavailableException e) {
            return PmsStatus.SHUTTING_DOWN;
        }
    }

    @Override
    public RawLookupResult getLocal(byte[] key) {
        try {
            return runtime.getLocalRaw(key);
        } catch (PmsServiceUnavailableException e) {
            return RawLookupResult.failed(PmsStatus.SHUTTING_DOWN);
        }
    }

    @Override
    public RawLookupResult getFull(byte[] key) {
        try {
            return runtime.getFullRaw(key);
        } catch (PmsLookupUnavailableException e) {
            return RawLookupResult.lookupUnavailable();
        } catch (PmsServiceUnavailableException e) {
            return RawLookupResult.failed(PmsStatus.SHUTTING_DOWN);
        }
    }

    @Override
    public RawLookupBatchResult getPrefixLocal(byte[] prefix) {
        try {
            return runtime.prefixLocalRaw(prefix);
        } catch (PmsServiceUnavailableException e) {
            return RawLookupBatchResult.failed(PmsStatus.SHUTTING_DOWN);
        }
    }
}
