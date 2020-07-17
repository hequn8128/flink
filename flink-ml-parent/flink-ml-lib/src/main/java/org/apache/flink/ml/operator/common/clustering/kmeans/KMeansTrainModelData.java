package org.apache.flink.ml.operator.common.clustering.kmeans;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.operator.common.clustering.DistanceType;
import org.apache.flink.ml.params.shared.HasVectorSizeDv100;
import org.apache.flink.ml.params.shared.clustering.KMeans4LongiLatitudeTrainParams;
import org.apache.flink.ml.params.shared.clustering.KMeansTrainParams;

import java.io.Serializable;
import java.util.List;

/**
 * Model data for KMeans trainData.
 */
public class KMeansTrainModelData implements Serializable {
    public List<ClusterSummary> centroids;
    public ParamSummary params;

    public long getClusterId(int clusterIndex) {
        return centroids.get(clusterIndex).clusterId;
    }

    public double getClusterWeight(int clusterIndex) {
        return centroids.get(clusterIndex).weight;
    }

    public DenseVector getClusterVector(int clusterIndex) {
        return centroids.get(clusterIndex).vec;
    }

    public void setClusterWeight(int clusterIndex, double weight) {
        centroids.get(clusterIndex).weight = weight;
    }

    public static class ClusterSummary implements Serializable {
        /**
         * Cluster Id.
         */
        private long clusterId;

        /**
         * Cluster Weight.
         */
        private double weight;

        /**
         * Cluster vector.
         */
        private DenseVector vec;

        public ClusterSummary(){}

        public ClusterSummary(DenseVector vec, long clusterId, double weight) {
            this.vec = vec;
            this.clusterId = clusterId;
            this.weight = weight;
        }
    }

    public static class ParamSummary implements Serializable {
        /**
         * Cluster number.
         */
        public int k;

        /**
         * Cluster vector size.
         */
        public int vectorSize;

        /**
         * DistanceType.
         */
        public DistanceType distanceType;

        /**
         * Vector column name.
         */
        public String vectorColName;

        /**
         * Latitude column name.
         */
        public String latitudeColName;

        /**
         * Longtitude column name.
         */
        public String longtitudeColName;

        public ParamSummary() {}

        public ParamSummary(Params params) {
            k = params.get(KMeansTrainParams.K);
            vectorSize = params.get(HasVectorSizeDv100.VECTOR_SIZE);
            distanceType = DistanceType.valueOf(params.get(KMeansTrainParams.DISTANCE_TYPE).toUpperCase());
            vectorColName = params.get(KMeansTrainParams.VECTOR_COL);
            latitudeColName = params.get(KMeans4LongiLatitudeTrainParams.LATITUDE_COL);
            longtitudeColName = params.get(KMeans4LongiLatitudeTrainParams.LONGITUDE_COL);
        }

        public Params toParams() {
            return new Params().set(KMeansTrainParams.DISTANCE_TYPE, distanceType.name())
                .set(KMeansTrainParams.K, k)
                .set(HasVectorSizeDv100.VECTOR_SIZE, vectorSize)
                .set(KMeansTrainParams.VECTOR_COL, vectorColName)
                .set(KMeans4LongiLatitudeTrainParams.LATITUDE_COL, latitudeColName)
                .set(KMeans4LongiLatitudeTrainParams.LONGITUDE_COL, longtitudeColName);
        }
    }
}
