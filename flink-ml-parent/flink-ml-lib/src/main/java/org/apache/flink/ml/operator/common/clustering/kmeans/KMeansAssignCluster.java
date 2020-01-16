package org.apache.flink.ml.operator.common.clustering.kmeans;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.comqueue.ComContext;
import org.apache.flink.ml.common.comqueue.ComputeFunction;
import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.operator.batch.clustering.KMeansTrainBatchOp;
import org.apache.flink.ml.operator.common.distance.FastDistance;
import org.apache.flink.ml.operator.common.distance.FastDistanceMatrixData;
import org.apache.flink.ml.operator.common.distance.FastDistanceVectorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find the closest cluster for every point and calculate the sums of the points belonging to the same cluster.
 */
public class KMeansAssignCluster extends ComputeFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KMeansAssignCluster.class);
    private FastDistance fastDistance;
    private transient DenseMatrix distanceMatrix;

    public KMeansAssignCluster(FastDistance fastDistance) {
        this.fastDistance = fastDistance;
    }

    @Override
    public void calc(ComContext context) {
        LOG.info("StepNo {}, TaskId {} Assign cluster begins!", context.getStepNo(),
            context.getTaskId());

        Integer vectorSize = context.getObj(KMeansTrainBatchOp.VECTOR_SIZE);
        Integer k = context.getObj(KMeansTrainBatchOp.K);
        // get iterative coefficient from static memory.
        Tuple2<Integer, FastDistanceMatrixData> stepNumCentroids;
        if (context.getStepNo() % 2 == 0) {
            stepNumCentroids = context.getObj(KMeansTrainBatchOp.CENTROID1);
        } else {
            stepNumCentroids = context.getObj(KMeansTrainBatchOp.CENTROID2);
        }

        if (null == distanceMatrix) {
            distanceMatrix = new DenseMatrix(k, 1);
        }

        double[] sumMatrixData = context.getObj(KMeansTrainBatchOp.CENTROID_ALL_REDUCE);
        if (sumMatrixData == null) {
            sumMatrixData = new double[k * (vectorSize + 1)];
            context.putObj(KMeansTrainBatchOp.CENTROID_ALL_REDUCE, sumMatrixData);
        }

        Iterable<FastDistanceVectorData> trainData = context.getObj(KMeansTrainBatchOp.TRAIN_DATA);
        if (trainData == null) {
            return;
        }

        for (FastDistanceVectorData sample : trainData) {
            KMeansUtil.updateSumMatrix(sample, 1, stepNumCentroids.f1, vectorSize, sumMatrixData, k, fastDistance,
                distanceMatrix);
        }
        LOG.info("StepNo {}, TaskId {} Assign cluster ends!", context.getStepNo(),
            context.getTaskId());

    }

}
