package org.qwh.pms.server;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaimonTableLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonTableLoader.class);

    public LoadedTable load(PmsServerConfig config) throws Exception {
        String warehouse = config.coreConfig().paimon().warehouse();
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalArgumentException("Missing required config key: pms.paimon.warehouse");
        }
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        Identifier identifier = Identifier.create(config.database(), config.table());
        Table table = catalog.getTable(identifier);
        LOG.info(
            "Loaded Paimon table: warehouse={}, identifier={}, primaryKeys={}, rowType={}",
            warehouse,
            identifier,
            table.primaryKeys(),
            table.rowType()
        );
        return new LoadedTable(catalog, table);
    }

    public record LoadedTable(Catalog catalog, Table table) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
