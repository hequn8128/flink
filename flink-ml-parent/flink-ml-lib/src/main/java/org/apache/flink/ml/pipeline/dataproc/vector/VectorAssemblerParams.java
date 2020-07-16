package org.apache.flink.ml.pipeline.dataproc.vector;

import org.apache.flink.ml.api.param.ParamInfo;
import org.apache.flink.ml.api.param.ParamInfoFactory;
import org.apache.flink.ml.params.mapper.MISOMapperParams;

/**
 * parameters of vector assembler.
 *
 */
public interface VectorAssemblerParams<T> extends
	MISOMapperParams<T> {

	ParamInfo<String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", String.class)
		.setDescription("parameter for how to handle invalid data (NULL values)")
		.setHasDefaultValue("error")
		.build();

	/**
	 * parameter how to handle invalid data (NULL values). Options are 'skip' (filter out rows with
	 * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the output).
	 */
	default String getHandleInvalid() {return get(HANDLE_INVALID);}

	default T setHandleInvalid(String value) {return set(HANDLE_INVALID, value);}
}
