package org.qwh.pms.core.wal;

public interface WALManager {

    long appendDataRecord(byte[] key, byte[] value);

    long lastSequenceId();

    void replay(ReplayCallback callback);

    void truncate(long safeSequenceId);

    void close();
}
