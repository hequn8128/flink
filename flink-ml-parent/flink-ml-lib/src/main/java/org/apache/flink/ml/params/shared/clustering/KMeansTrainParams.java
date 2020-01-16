package org.apache.flink.ml.params.shared.clustering;

import org.apache.flink.ml.params.shared.HasDistanceType;
import org.apache.flink.ml.params.shared.colname.HasVectorCol;

/**
 * Params for KMeansTrainer.
 */
public interface KMeansTrainParams<T> extends
	HasDistanceType<T>,
	HasVectorCol<T>,
	BaseKMeansTrainParams <T> {
}
