package org.qwh.pms.core.wal;

public interface WALManager {

    long appendDataRecord(byte[] key, byte[] value);

    long lastSequenceId();

    void appendSinkPrepare(byte[] commitMessage);

    void appendSinkSuccess(long snapshotId);

    void replay(ReplayCallback callback, long highWatermarkSnapshotId);

    void truncate(long safeSnapshotId);

    void close();
}
