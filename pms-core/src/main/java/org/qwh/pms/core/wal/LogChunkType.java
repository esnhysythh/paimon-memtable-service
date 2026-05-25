package org.qwh.pms.core.wal;

public enum LogChunkType
{
    ZERO_TYPE(0),
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4),
    EOF,
    BAD_CHUNK,
    UNKNOWN;

    public static LogChunkType getLogChunkTypeByPersistentId(int persistentId)
    {
        for (LogChunkType logChunkType : LogChunkType.values()) {
            if (logChunkType.persistentId != null && logChunkType.persistentId == persistentId) {
                return logChunkType;
            }
        }
        return UNKNOWN;
    }

    private final Integer persistentId;

    LogChunkType()
    {
        this.persistentId = null;
    }

    LogChunkType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        if (persistentId == null) {
            throw new IllegalArgumentException(name() + " is not a persistent chunk type");
        }
        return persistentId;
    }
}
