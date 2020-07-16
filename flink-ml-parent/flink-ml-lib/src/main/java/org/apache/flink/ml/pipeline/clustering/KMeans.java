/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.batch.clustering.KMeansTrainBatchOp;
import org.apache.flink.ml.params.shared.clustering.KMeansPredictParams;
import org.apache.flink.ml.params.shared.clustering.KMeansTrainParams;
import org.apache.flink.ml.pipeline.Trainer;

/**
 * k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 * analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 * observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 */
public class KMeans extends Trainer<KMeans, KMeansModel> implements
	KMeansTrainParams <KMeans>,
	KMeansPredictParams <KMeans> {

	public KMeans() {
		super();
	}

	public KMeans(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new KMeansTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
