package org.apache.flink.ml.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.table.api.Table;

import java.lang.reflect.ParameterizedType;

/**
 * Abstract class for a trainer that train a machine learning model.
 *
 * The different between {@link EstimatorBase} and {@link Trainer} is that
 * some of {@link EstimatorBase} have its own feature such as some ensemble algorithms,
 * some frequent item set mining algorithms, auto tuning, etc.
 *
 * @param <T> The class type of the {@link Trainer} implementation itself
 * @param <M> class type of the {@link ModelBase} this Trainer produces.
 */
public abstract class Trainer<T extends Trainer <T, M>, M extends ModelBase<M>>
	extends EstimatorBase<T, M> {

	public Trainer() {
		super();
	}

	public Trainer(Params params) {
		super(params);
	}

	@Override
	public M fit(BatchOperator input) {
		return createModel(train(input).getOutput());
	}

	@Override
	public M fit(StreamOperator input) {
		return createModel(train(input).getOutput());
	}

	private M createModel(Table model) {
		try {
			ParameterizedType pt =
				(ParameterizedType) this.getClass().getGenericSuperclass();

			Class <M> classM = (Class <M>) pt.getActualTypeArguments()[1];

			return (M) classM.getConstructor(Params.class)
				.newInstance(getParams())
				.setModelData(model);

		} catch (Exception ex) {
			throw new RuntimeException(ex.toString());
		}
	}

	protected abstract BatchOperator train(BatchOperator in);

	protected StreamOperator train(StreamOperator in) {
		throw new UnsupportedOperationException("Only support batch fit!");
	}

}
