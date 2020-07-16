package org.apache.flink.ml.operator.common.clustering.kmeans;

import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.operator.common.distance.FastDistanceMatrixData;

/**
 * Model Data used for KMeans prediction.
 */
public class KMeansPredictModelData {
    public FastDistanceMatrixData centroids;
    public KMeansTrainModelData.ParamSummary params;

    public long getClusterId(int clusterIndex){
        return (long)centroids.getRows()[clusterIndex].getField(0);
    }

    public double getClusterWeight(int clusterIndex){
        return (double)centroids.getRows()[clusterIndex].getField(1);
    }

    public DenseVector getClusterVector(int clusterIndex){
        return new DenseVector(centroids.getVectors().getColumn(clusterIndex));
    }
}
