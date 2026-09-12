package org.qwh.pms.testkit.paimon;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import java.util.Locale;
import java.util.Objects;

public record PaimonTestTableSpec(String database, String table) {

    public PaimonTestTableSpec {
        database = identifier(database, "database");
        table = identifier(table, "table");
    }

    public static PaimonTestTableSpec forRun(String runId, String scenario) {
        return new PaimonTestTableSpec(
            shorten("pms_it_" + normalize(runId), 60),
            shorten(normalize(scenario), 60)
        );
    }

    public Identifier identifier() {
        return Identifier.create(database, table);
    }

    public Schema schema() {
        return Schema.newBuilder()
            .column("id", DataTypes.BIGINT().notNull())
            .column("payload", DataTypes.STRING())
            .column("version", DataTypes.INT())
            .primaryKey("id")
            .option("bucket", "1")
            .option("merge-engine", "deduplicate")
            .option("file.format", "parquet")
            .build();
    }

    private static String identifier(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (!value.matches("[a-z][a-z0-9_]{0,62}")) {
            throw new IllegalArgumentException(
                name + " must match [a-z][a-z0-9_]{0,62}: " + value
            );
        }
        return value;
    }

    private static String normalize(String value) {
        Objects.requireNonNull(value, "identifier source must not be null");
        String normalized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
        normalized = normalized.replaceAll("^_+|_+$", "");
        if (normalized.isEmpty()) {
            normalized = "run";
        }
        if (!Character.isLetter(normalized.charAt(0))) {
            normalized = "r_" + normalized;
        }
        return normalized;
    }

    private static String shorten(String value, int maxLength) {
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
