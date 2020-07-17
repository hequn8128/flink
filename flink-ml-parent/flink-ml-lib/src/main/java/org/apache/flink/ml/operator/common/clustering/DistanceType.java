package org.apache.flink.ml.operator.common.clustering;

import org.apache.flink.ml.operator.common.distance.ContinuousDistance;
import org.apache.flink.ml.operator.common.distance.CosineDistance;
import org.apache.flink.ml.operator.common.distance.EuclideanDistance;
import org.apache.flink.ml.operator.common.distance.HaversineDistance;
import org.apache.flink.ml.operator.common.distance.JaccardDistance;
import org.apache.flink.ml.operator.common.distance.ManHattanDistance;

import java.io.Serializable;

/**
 * Various distance types.
 */
public enum DistanceType implements Serializable {
	/**
	 * EUCLIDEAN
	 */
	EUCLIDEAN(new EuclideanDistance()),
	/**
	 * COSINE
	 */
	COSINE(new CosineDistance()),
	/**
	 * CITYBLOCK
	 */
	CITYBLOCK(new ManHattanDistance()),
	/**
	 * HAVERSINE
	 */
	HAVERSINE(new HaversineDistance()),

	/**
	 * JACCARD
	 */
	JACCARD(new JaccardDistance());

	public ContinuousDistance getContinuousDistance() {
		return continuousDistance;
	}

	private ContinuousDistance continuousDistance;

	DistanceType(ContinuousDistance continuousDistance){
		this.continuousDistance = continuousDistance;
	}


}
