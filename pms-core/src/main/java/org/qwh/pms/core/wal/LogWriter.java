package org.qwh.pms.core.wal;

import org.qwh.pms.core.wal.util.Slice;

import java.io.File;
import java.io.IOException;

public interface LogWriter
{
    boolean isClosed();

    void close()
            throws IOException;

    void delete()
            throws IOException;

    File getFile();

    long getFileNumber();

    void addRecord(Slice record, boolean force)
            throws IOException;
}
