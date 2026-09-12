package org.qwh.pms.testkit.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;

import java.io.IOException;
import java.net.URI;

public final class PaimonCatalogs {
    private PaimonCatalogs() {}

    static Catalog open(PmsTestEnvironment environment, URI warehouse) {
        return CatalogFactory.createCatalog(context(environment, warehouse));
    }

    public static FileIO fileIO(PmsTestEnvironment environment, URI path) throws IOException {
        return FileIO.get(new Path(path), context(environment, path));
    }

    private static CatalogContext context(PmsTestEnvironment environment, URI warehouse) {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        options.setString("hadoop-conf-dir", environment.hadoopConfDir().toString());
        return CatalogContext.create(options);
    }
}
