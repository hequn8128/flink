package org.apache.flink.ml.params.mapper;

import org.apache.flink.ml.params.shared.colname.HasPredictionCol;
import org.apache.flink.ml.params.shared.colname.HasPredictionDetailCol;
import org.apache.flink.ml.params.shared.colname.HasReservedCols;

/**
 * Params for RichModelMapper.
 */
public interface RichModelMapperParams<T> extends
	HasPredictionCol<T>,
	HasPredictionDetailCol<T>,
	HasReservedCols<T> {
}
