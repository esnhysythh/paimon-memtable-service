package org.qwh.pms.core.wal;

import org.qwh.pms.core.config.PMSConfig;

public class WalConfig
{
    private final boolean useMmap;

    public WalConfig(PMSConfig config)
    {
        this.useMmap = config.walUseMmap();
    }

    public boolean useMmap()
    {
        return useMmap;
    }
}
