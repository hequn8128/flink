package org.apache.flink.ml.params.mapper;

import org.apache.flink.ml.params.shared.colname.HasOutputCol;
import org.apache.flink.ml.params.shared.colname.HasReservedCols;
import org.apache.flink.ml.params.shared.colname.HasSelectedCols;

/**
 * Parameters for MISOMapper.
 *
 * @param <T>
 */
public interface MISOMapperParams<T> extends
	HasSelectedCols<T>,
	HasOutputCol<T>,
	HasReservedCols<T> {
}
