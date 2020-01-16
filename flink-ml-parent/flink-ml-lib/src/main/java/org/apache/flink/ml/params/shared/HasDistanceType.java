package org.apache.flink.ml.params.shared;

import org.apache.flink.ml.api.param.ParamInfo;
import org.apache.flink.ml.api.param.ParamInfoFactory;
import org.apache.flink.ml.api.param.WithParams;
import org.apache.flink.ml.operator.common.clustering.DistanceType;

/**
 * Params: Distance type for clustering, support EUCLIDEAN and COSINE.
 */
public interface HasDistanceType<T> extends WithParams<T> {
	ParamInfo<String> DISTANCE_TYPE = ParamInfoFactory
		.createParamInfo("distanceType", String.class)
		.setDescription("Distance type for clustering, support EUCLIDEAN and COSINE.")
		.setHasDefaultValue(DistanceType.EUCLIDEAN.name())
		.setAlias(new String[]{"metric"})
		.build();

	default String getDistanceType() {return get(DISTANCE_TYPE);}

	default T setDistanceType(String value) {return set(DISTANCE_TYPE, value);}
}
