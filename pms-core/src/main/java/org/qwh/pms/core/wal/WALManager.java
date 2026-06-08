package org.qwh.pms.core.wal;

import java.util.List;

public interface WALManager {

    long appendDataRecord(byte[] key, byte[] value);

    long appendDataRecords(List<DataWrite> writes);

    long lastSequenceId();

    void replay(ReplayCallback callback);

    void truncate(long safeSequenceId);

    void close();

    record DataWrite(byte[] key, byte[] value) {}
}
