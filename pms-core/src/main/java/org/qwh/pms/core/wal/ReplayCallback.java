package org.qwh.pms.core.wal;

public interface ReplayCallback {

    void onDataRecord(byte[] key, byte[] value);

    default void onDataRecord(long sequenceId, byte[] key, byte[] value) {
        onDataRecord(key, value);
    }

    void onSinkPrepare(byte[] commitMessage);

    void onSinkSuccess(long snapshotId);

    default void onSinkSuccess(long snapshotId, byte[] metadata) {
        onSinkSuccess(snapshotId);
    }
}
