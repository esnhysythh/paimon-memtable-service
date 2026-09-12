package org.qwh.pms.testkit.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;

import java.net.URI;
import java.util.Objects;

public final class PaimonTestTable implements AutoCloseable {
    private final Catalog catalog;
    private final Table table;

    private PaimonTestTable(Catalog catalog, Table table) {
        this.catalog = catalog;
        this.table = table;
    }

    public static PaimonTestTable create(
            PmsTestEnvironment environment,
            URI warehouse,
            PaimonTestTableSpec spec) throws Exception {
        Objects.requireNonNull(spec, "spec must not be null");
        Catalog catalog = PaimonCatalogs.open(environment, warehouse);
        boolean success = false;
        try {
            catalog.createDatabase(spec.database(), false);
            catalog.createTable(spec.identifier(), spec.schema(), false);
            PaimonTestTable fixture = new PaimonTestTable(
                catalog,
                catalog.getTable(spec.identifier())
            );
            success = true;
            return fixture;
        } finally {
            if (!success) {
                catalog.close();
            }
        }
    }

    public Table table() {
        return table;
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
