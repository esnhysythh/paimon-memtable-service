package org.qwh.pms.lookup.api;

/** Raised when a data file cannot be decoded with the lookup's configured table schema. */
public final class SchemaMismatchException extends IllegalStateException {

    public SchemaMismatchException(String fileName, long expectedSchemaId, long actualSchemaId) {
        super(
                "Schema mismatch for Paimon data file "
                        + fileName
                        + ": expected schemaId="
                        + expectedSchemaId
                        + ", actual schemaId="
                        + actualSchemaId);
    }
}
