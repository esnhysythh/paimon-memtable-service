package org.qwh.pms.core.bucket;

import java.util.Optional;

public interface PMSBucketDirector {

    void put(byte[] key, byte[] value);

    void delete(byte[] key);

    Optional<byte[]> get(byte[] key);

    void freezeCurMemTable();

    BucketStateSnapshot stateSnapshot();

    void close();
}
