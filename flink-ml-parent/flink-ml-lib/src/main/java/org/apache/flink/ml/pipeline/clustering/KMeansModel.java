package org.apache.flink.ml.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.common.clustering.kmeans.KMeansModelMapper;
import org.apache.flink.ml.params.shared.clustering.KMeansPredictParams;
import org.apache.flink.ml.pipeline.MapModel;

/**
 * Find  the closest cluster center for every point.
 */
public class KMeansModel extends MapModel<KMeansModel>
	implements KMeansPredictParams<KMeansModel> {

	public KMeansModel() {this(null);}

	public KMeansModel(Params params) {
		super(KMeansModelMapper::new, params);
	}

}
