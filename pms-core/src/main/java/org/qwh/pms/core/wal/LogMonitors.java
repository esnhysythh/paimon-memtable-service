package org.qwh.pms.core.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogMonitors
{
    private static final Logger LOG = LoggerFactory.getLogger(LogMonitors.class);

    public static LogMonitor throwExceptionMonitor()
    {
        return new LogMonitor()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes", bytes), reason);
            }
        };
    }

    public static LogMonitor logMonitor()
    {
        return new LogMonitor()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                LOG.warn("corruption of {} bytes: {}", bytes, reason);
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                LOG.warn("corruption of {} bytes", bytes, reason);
            }
        };
    }

    private LogMonitors()
    {
    }
}
