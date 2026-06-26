package org.qwh.pms.lookup.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import org.qwh.pms.lookup.api.LookupRequest;

/** Copies full or projected Paimon value rows for lookup results. */
final class PaimonValueProjector {

    private final RowType valueType;

    PaimonValueProjector(RowType valueType) {
        this.valueType = valueType;
    }

    InternalRow copy(InternalRow value, LookupRequest request) {
        return request.projection()
                .map(
                        projection -> {
                            RowType projectedType = valueType.project(projection);
                            ProjectedRow projectedRow = ProjectedRow.from(projection).replaceRow(value);
                            return new InternalRowSerializer(projectedType).copy(projectedRow);
                        })
                .orElseGet(() -> new InternalRowSerializer(valueType).copy(value));
    }
}
