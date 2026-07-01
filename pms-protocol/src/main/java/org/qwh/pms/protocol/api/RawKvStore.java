package org.qwh.pms.protocol.api;

import java.util.List;

public interface RawKvStore {

    PmsStatus put(byte[] key, byte[] row);

    PmsStatus delete(byte[] key);

    PmsStatus writeBatch(List<RawKvEntry> entries);

    RawLookupResult getLocal(byte[] key);

    RawLookupResult getFull(byte[] key);

    RawLookupBatchResult getPrefixLocal(byte[] prefix);
}
