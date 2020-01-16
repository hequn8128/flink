package org.apache.flink.ml.params.shared.clustering;

import org.apache.flink.ml.api.param.ParamInfo;
import org.apache.flink.ml.api.param.ParamInfoFactory;

/**
 * Params for KMeans4LongiLatitudeTrainer.
 */
public interface KMeans4LongiLatitudeTrainParams<T> extends
	BaseKMeansTrainParams <T> {
	ParamInfo<String> LONGITUDE_COL = ParamInfoFactory
		.createParamInfo("longitudeCol", String.class)
		.setDescription("Longitude colname")
		.setAlias(new String[]{"longitudeColName"})
		.setRequired()
		.build();
	ParamInfo<String> LATITUDE_COL = ParamInfoFactory
		.createParamInfo("latitudeCol", String.class)
		.setDescription("Latitude colname")
		.setAlias(new String[]{"latitudeColName"})
		.setRequired()
		.build();

	default String getLongitudeCol() {
		return get(LONGITUDE_COL);
	}

	default T setLongitudeCol(String value) {
		return set(LONGITUDE_COL, value);
	}

	default String getLatitudeCol() {
		return get(LATITUDE_COL);
	}

	default T setLatitudeCol(String value) {
		return set(LATITUDE_COL, value);
	}
}
