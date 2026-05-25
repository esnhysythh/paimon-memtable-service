package org.qwh.pms.core.wal;

public interface LogMonitor
{
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
