package org.qwh.pms.core.wal;

public final class LogConstants
{
    public static final int BLOCK_SIZE = 32768;

    public static final int HEADER_SIZE = 4 + 1 + 2; // CRC32C + ChunkType + Length

    private LogConstants()
    {
    }
}
