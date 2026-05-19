package org.qwh.pms.core.wal;

public interface ReplayCallback {

    void onDataRecord(byte[] key, byte[] value);

    void onSinkPrepare(byte[] commitMessage);

    void onSinkSuccess(long snapshotId);
}
