package org.qwh.pms.sink.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

final class PaimonSinkTableValidator {
    private PaimonSinkTableValidator() {}

    static void validate(Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException("Paimon sink requires a FileStoreTable");
        }
        if (table.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException("Paimon sink requires a primary-key table");
        }
        CoreOptions.MergeEngine mergeEngine = ((DataTable) table).coreOptions().mergeEngine();
        if (mergeEngine != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new IllegalArgumentException(
                "Paimon sink requires merge-engine=deduplicate, actual=" + mergeEngine
            );
        }
    }
}
