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

package org.apache.flink.ml.examples;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.ml.pipeline.clustering.KMeans;
import org.apache.flink.ml.pipeline.dataproc.VectorAssembler;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;


/**
 * Pipeline Example for KMeans.
 */
public class KMeansExample {

	public static void main(String[] args) throws Exception {
		// Init env
		BatchTableEnvironment batchTableEnvironment = MLEnvironmentFactory.getDefault().getBatchTableEnvironment();

		// init source
		String[] inputNames = new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width", "category"};
		TypeInformation<?>[] inputTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING};
		CsvTableSource csvTableSource = new CsvTableSource("/Users/hequn.chq/Downloads/iris.csv", inputNames, inputTypes);
		MLEnvironmentFactory.getDefault().getBatchTableEnvironment().registerTableSource("source", csvTableSource);

		Table sourceTable = batchTableEnvironment.scan("source");

		// transformer
		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
			.setOutputCol("features");

		// estimator
		KMeans kMeans = new KMeans().setVectorCol("features").setK(3)
			.setPredictionCol("prediction_result")
			.setPredictionDetailCol("prediction_detail")
			.setReservedCols("category")
			.setMaxIter(100);

		// define pipeline
		Pipeline pipeline = new Pipeline()
			.appendStage(va)
			.appendStage(kMeans);

		Table resultTable = pipeline
			.fit(batchTableEnvironment, sourceTable)
			.transform(batchTableEnvironment, sourceTable);

		resultTable.printSchema();

		// output result
		batchTableEnvironment.toDataSet(resultTable, Row.class).print();
	}

//	public static void main(String[] args) throws Exception {
//
//		// Init env
//		BatchTableEnvironment batchTableEnvironment = MLEnvironmentFactory.getDefault().getBatchTableEnvironment();
//
//		// init source
//		String[] inputNames = new String[] {"a", "b"};
//		TypeInformation<?>[] inputTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE};
//		CsvTableSource csvTableSource = new CsvTableSource("/Users/hequn.chq/Downloads/a", inputNames, inputTypes);
//		MLEnvironmentFactory.getDefault().getBatchTableEnvironment().registerTableSource("source", csvTableSource);
//
//		Table sourceTable = batchTableEnvironment.scan("source");
//
//		// transformer
//		Params vaParams = new Params();
//		vaParams.set(VectorAssembler.SELECTED_COLS, new String[]{"a", "b"})
//			.set(VectorAssembler.OUTPUT_COL, "features");
//		VectorAssembler va = new VectorAssembler(vaParams);
//
//		// estimator
//		KMeans kMeans = new KMeans().setVectorCol("features").setK(2)
//			.setPredictionCol("prediction_result")
//			.setReservedCols(new String[]{"a", "b"});
//
//		// define pipeline
//		Pipeline pipeline = new Pipeline()
//			.appendStage(va)
//			.appendStage(kMeans);
//
//		Table resultTable = pipeline
//			.fit(batchTableEnvironment, sourceTable)
//			.transform(batchTableEnvironment, sourceTable);
//
//		resultTable.printSchema();
//
//		// output result
//		batchTableEnvironment.toDataSet(resultTable, Row.class).print();
//	}
}
