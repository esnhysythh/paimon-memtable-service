package org.qwh.pms.server;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.qwh.pms.core.config.PaimonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaimonTableLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonTableLoader.class);

    public LoadedTable load(PmsServerConfig config) throws Exception {
        PaimonConfig paimon = config.coreConfig().paimon();
        String warehouse = paimon.warehouse();
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalArgumentException("Missing required config key: pms.paimon.warehouse");
        }
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions(paimon)));
        Identifier identifier = Identifier.create(config.database(), config.table());
        Table table = catalog.getTable(identifier);
        LOG.info(
            "Loaded Paimon table: warehouse={}, identifier={}, primaryKeys={}, rowType={}, manifestCacheEnabled={}",
            warehouse,
            identifier,
            table.primaryKeys(),
            table.rowType(),
            table instanceof FileStoreTable fileStoreTable && fileStoreTable.getManifestCache() != null
        );
        return new LoadedTable(catalog, table);
    }

    private static Options catalogOptions(PaimonConfig paimon) {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, paimon.warehouse());
        options.set(CatalogOptions.CACHE_ENABLED, paimon.cacheEnabled());
        options.setString("cache.manifest.small-file-memory", paimon.manifestCacheSmallFileMemory());
        options.setString("cache.manifest.small-file-threshold", paimon.manifestCacheSmallFileThreshold());
        if (paimon.manifestCacheMaxMemory() != null) {
            options.setString("cache.manifest.max-memory", paimon.manifestCacheMaxMemory());
        }
        return options;
    }

    public record LoadedTable(Catalog catalog, Table table) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
