package org.qwh.pms.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public final class PmsServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerMain.class);

    private PmsServerMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: PmsServerMain <pms-server.properties>");
        }
        PmsServerConfig config = new ConfigManager().load(Path.of(args[0]));
        PmsServerRuntime runtime = new PmsServerRuntime(config).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("PMS server shutdown requested");
            try {
                runtime.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("PMS server shutdown completed");
        }));
        LOG.info("PMS server listening on http://{}:{}", config.host(), runtime.port());
    }
}
