/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ai.tensorflow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonScalarFunction;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperator;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/**
 * xxx.
 */
public class TFCluster {

	public static void run(
//		StreamExecutionEnvironment env,
//		StreamTableEnvironment tableEnv,
//		Table t,
		PythonScalarFunction mapFunc,
		PythonScalarFunction serverFunc) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// init left ds
		DataStream ds1 = env.addSource(getSourceFunction(2)).setParallelism(1)
			.map(new MapFunction<Integer, Row>() {
				@Override
				public Row map(Integer value) throws Exception {
					Row row = new Row(1);
					row.setField(0, value);
					return row;
				}
			});

		// connect
		ds1.connect(getBroadcastStream(env, serverFunc))
			.transform("connect", new RowTypeInfo(Types.INT), getConnectOperator(mapFunc)).setParallelism(2)
//			.process(getCoProcessFunction(mapFunc)).setParallelism(2)
			.print().setParallelism(2);

		env.execute();
	}

	public static PythonFunctionTwoInputStreamOperator getConnectOperator(PythonScalarFunction mapFunc) {
		PythonFunctionInfo pythonFunctionInfo1 = new PythonFunctionInfo(mapFunc, new Integer[] {0, 1});
		return new PythonFunctionTwoInputStreamOperator(
				new Configuration(),
				new PythonFunctionInfo[] {pythonFunctionInfo1},
				RowType.of(new IntType()),
				RowType.of(new IntType())
			);
	}

	public static DataStream<Row> getBroadcastStream(
		StreamExecutionEnvironment env,
		PythonScalarFunction serverFunc) {
		PythonFunctionInfo pythonFunctionInfo = new PythonFunctionInfo(serverFunc, new Integer[] {0});
		PythonScalarFunctionOperator pythonScalarFunctionOperator =
			new PythonScalarFunctionOperator(
				new Configuration(),
				new PythonFunctionInfo[] {pythonFunctionInfo},
				RowType.of(new IntType()),
				RowType.of(new VarCharType()),
				new int[] {0},
				new int[] {}
			);

		return env.addSource(getSourceFunction(1)).setParallelism(1)
			.map(new MapFunction<Integer, CRow>() {
				@Override
				public CRow map(Integer value) throws Exception {
					Row row = new Row(1);
					row.setField(0, value);
					return new CRow(row, true);
				}
			})
			.transform("serverMap", new CRowTypeInfo(new RowTypeInfo(Types.STRING)), pythonScalarFunctionOperator).setParallelism(1)
			.map(new MapFunction<CRow, Row>() {
				@Override
				public Row map(CRow value) throws Exception {
					return value.row();
				}
			})
			.setParallelism(1).broadcast();
	}

	public static SourceFunction<Integer> getSourceFunction(int number) {
		return new SourceFunction<Integer>() {
			private volatile boolean isRunning = true;

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {

				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < number; ++i) {
						ctx.collect(Integer.valueOf(i));
					}
				}

				while (isRunning) {
					Thread.sleep(1000);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		};
	}
}
