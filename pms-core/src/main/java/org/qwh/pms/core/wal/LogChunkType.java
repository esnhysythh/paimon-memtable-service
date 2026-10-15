package org.qwh.pms.core.wal;

public enum LogChunkType
{
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4);

    private final int persistentId;

    LogChunkType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
