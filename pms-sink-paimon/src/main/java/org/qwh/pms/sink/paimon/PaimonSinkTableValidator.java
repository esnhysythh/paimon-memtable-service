package org.qwh.pms.sink.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.List;

final class PaimonSinkTableValidator {
    private PaimonSinkTableValidator() {}

    static void validate(Table table) {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new IllegalArgumentException("Paimon sink requires a FileStoreTable");
        }
        if (table.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException("Paimon sink requires a primary-key table");
        }
        List<String> partitionKeys = table.partitionKeys();
        if (!table.primaryKeys().containsAll(partitionKeys)) {
            throw new IllegalArgumentException(
                "Paimon sink rejects Cross Partitions Upsert; primary keys must contain all partition fields, "
                    + "partitionKeys="
                    + partitionKeys
                    + ", primaryKeys="
                    + table.primaryKeys()
            );
        }
        BucketMode bucketMode = fileStoreTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new IllegalArgumentException(
                "Paimon sink requires HASH_FIXED buckets, actual=" + bucketMode
            );
        }
        CoreOptions.MergeEngine mergeEngine = fileStoreTable.coreOptions().mergeEngine();
        if (mergeEngine != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new IllegalArgumentException(
                "Paimon sink requires merge-engine=deduplicate, actual=" + mergeEngine
            );
        }
    }
}
