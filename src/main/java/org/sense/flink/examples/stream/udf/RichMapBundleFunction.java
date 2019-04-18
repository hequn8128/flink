package org.sense.flink.examples.stream.udf;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 *
 * @param <K> The type of the key in the bundle map
 * @param <V> The type of the value in the bundle map
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class RichMapBundleFunction<K, V, IN, OUT> extends AbstractRichFunction implements Function {

	private static final long serialVersionUID = -1247262047806613407L;

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 */
	public abstract V addInput(@Nullable V value, IN input);

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more
	 * output elements.
	 */
	public abstract void finishBundle(Map<K, V> buffer, Collector<OUT> out) throws Exception;

}
