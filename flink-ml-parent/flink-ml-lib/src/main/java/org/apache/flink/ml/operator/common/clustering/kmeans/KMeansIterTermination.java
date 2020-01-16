package org.apache.flink.ml.operator.common.clustering.kmeans;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.comqueue.ComContext;
import org.apache.flink.ml.common.comqueue.CompareCriterionFunction;
import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.operator.batch.clustering.KMeansTrainBatchOp;
import org.apache.flink.ml.operator.common.distance.FastDistance;
import org.apache.flink.ml.operator.common.distance.FastDistanceMatrixData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate the distance between the pre-round centers and current centers. Judge whether algorithm converges.
 */
public class KMeansIterTermination extends CompareCriterionFunction {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansIterTermination.class);
    private FastDistance distance;
    private double tol;
    private transient DenseMatrix distanceMatrix;

    public KMeansIterTermination(FastDistance distance, double tol) {
        this.distance = distance;
        this.tol = tol;
    }

    @Override
    public boolean calc(ComContext context) {
        Integer k = context.getObj(KMeansTrainBatchOp.K);
        Tuple2<Integer, FastDistanceMatrixData> centroids1 = context.getObj(KMeansTrainBatchOp.CENTROID1);
        Tuple2<Integer, FastDistanceMatrixData> centroids2 = context.getObj(KMeansTrainBatchOp.CENTROID2);

        distanceMatrix = distance.calc(centroids1.f1, centroids2.f1, distanceMatrix);

        for (int id = 0; id < k; id++) {
            double d = distanceMatrix.get(id, id);
            LOG.info("StepNo {}, TaskId {} ||centroid-prev_centroid|| {}",
                context.getStepNo(), context.getTaskId(), d);
            if (d >= tol) {
                return false;
            }
        }
        return true;
    }
}
