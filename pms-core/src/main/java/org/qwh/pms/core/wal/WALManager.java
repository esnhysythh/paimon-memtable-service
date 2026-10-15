package org.qwh.pms.core.wal;

import java.io.IOException;
import java.util.List;

public interface WALManager {

    long appendDataRecord(byte[] key, byte[] value);

    long appendDataRecords(List<DataWrite> writes);

    long lastSequenceId();

    void recover(long durableSequenceId, ReplayCallback callback) throws IOException;

    void truncate(long safeSequenceId);

    void close();

    record DataWrite(byte[] key, byte[] value) {}
}
