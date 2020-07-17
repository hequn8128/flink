package org.apache.flink.ml.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.mapper.Mapper;
import org.apache.flink.ml.common.mapper.MapperAdapter;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link StreamOperator}.
 *
 * @param <T> class type of the {@link MapStreamOp} implementation itself.
 */
public class MapStreamOp<T extends MapStreamOp<T>> extends StreamOperator<T> {

	private final BiFunction<TableSchema, Params, Mapper> mapperBuilder;

	public MapStreamOp(BiFunction<TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			DataStream<Row> resultRows = in.getDataStream().map(new MapperAdapter(mapper));
			this.setOutput(resultRows, mapper.getOutputSchema());

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
