/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.collect.CollectSinkAddressEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.JavaStreamTestData;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.apache.flink.util.Preconditions;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.api.Expressions.$;


class MyEvent implements OperatorEvent {
	private static final long serialVersionUID = 1L;

	private final String cluster;

	public MyEvent(String cluster) {
		this.cluster = cluster;
	}

	public String getCluster() {
		return cluster;
	}
}

class MyOperator extends AbstractStreamOperator<Integer>
	implements OneInputStreamOperator<Integer, Integer>, OperatorEventHandler {

	private transient OperatorEventGateway eventGateway;
	private InetSocketAddress address;

	@Override
	public void open() throws Exception {
		super.open();

		// sending socket server address to coordinator
		Preconditions.checkNotNull(eventGateway, "Operator event gateway hasn't been set");
		address = new InetSocketAddress("192.168.1.1", Math.abs(new Random().nextInt() % 1024));

		CollectSinkAddressEvent addressEvent = new CollectSinkAddressEvent(address);
		eventGateway.sendEventToCoordinator(addressEvent);
	}

	@Override
	public void processElement(StreamRecord<Integer> element) throws Exception {
		output.collect(element);
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		String str = address.toString() + ((MyEvent) evt).getCluster();
		System.out.println(str);
	}
}

class MyOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

	private String clusters = "";
	private int cnt = 0;
	private Context context;

	public MyOperatorCoordinator(Context context) {
		this.context = context;
	}

	@Override
	public CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request) {

		return null;
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		cnt ++;
		Preconditions.checkArgument(
			event instanceof CollectSinkAddressEvent, "Operator event must be a CollectSinkAddressEvent");
		InetSocketAddress address = ((CollectSinkAddressEvent) event).getAddress();
		clusters += address.toString();
		System.out.println(clusters);

		if (cnt == 4) {
			for (int i = 0; i < context.currentParallelism(); ++i)
			context.sendEvent(new MyEvent(clusters), i);
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {

	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		return CompletableFuture.completedFuture(new byte[]{});
	}

	@Override
	public void checkpointComplete(long checkpointId) {

	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {

	}

	public static class Provider implements OperatorCoordinator.Provider {

		private final OperatorID operatorId;

		public Provider(OperatorID operatorId) {
			this.operatorId = operatorId;
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public OperatorCoordinator create(Context context) {
			return new MyOperatorCoordinator(context);
		}
	}
}

class MyOperatorFactory implements OneInputStreamOperatorFactory<Integer, Integer>, CoordinatedOperatorFactory<Integer> {

	MyOperator myOperator;

	public MyOperatorFactory(MyOperator myOperator) {
		this.myOperator = myOperator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new MyOperatorCoordinator.Provider(operatorID);
	}

	@Override
	public <T extends StreamOperator<Integer>> T createStreamOperator(StreamOperatorParameters<Integer> parameters) {
		final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
		myOperator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
		myOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		eventDispatcher.registerEventHandler(operatorId, myOperator);
		return (T) myOperator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {

	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return ChainingStrategy.ALWAYS;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return MyOperator.class;
	}
}

/**
 * Integration tests for streaming SQL.
 */
public class JavaSqlITCase extends AbstractTestBase {

	@Test
	public void testCoordinate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		env.fromElements(1, 2, 3, 4)
			.transform("coordinateTestOperator", Types.INT, new MyOperatorFactory(new MyOperator()))
			.print();

		env.execute();
	}

	@Test
	public void testRowRegisterRowWithNames() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		StreamITCase.clear();

		List<Row> data = new ArrayList<>();
		data.add(Row.of(1, 1L, "Hi"));
		data.add(Row.of(2, 2L, "Hello"));
		data.add(Row.of(3, 2L, "Hello world"));

		TypeInformation<?>[] types = {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO};
		String[] names = {"a", "b", "c"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

		Table in = tableEnv.fromDataStream(ds, $("a"), $("b"), $("c"));
		tableEnv.registerTable("MyTableRow", in);

		String sqlQuery = "SELECT a,c FROM MyTableRow";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,Hi");
		expected.add("2,Hello");
		expected.add("3,Hello world");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testSelect() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds = JavaStreamTestData.getSmall3TupleDataSet(env);
		Table in = tableEnv.fromDataStream(ds, $("a"), $("b"), $("c"));
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT * FROM MyTable";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testFilter() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		StreamITCase.clear();

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = JavaStreamTestData.get5TupleDataStream(env);
		tableEnv.createTemporaryView("MyTable", ds, $("a"), $("b"), $("c"), $("d"), $("e"));

		String sqlQuery = "SELECT a, b, e FROM MyTable WHERE c < 4";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,1");
		expected.add("2,2,2");
		expected.add("2,3,1");
		expected.add("3,4,2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds1 = JavaStreamTestData.getSmall3TupleDataSet(env);
		Table t1 = tableEnv.fromDataStream(ds1, $("a"), $("b"), $("c"));
		tableEnv.registerTable("T1", t1);

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds2 = JavaStreamTestData.get5TupleDataStream(env);
		tableEnv.createTemporaryView("T2", ds2, $("a"), $("b"), $("d"), $("c"), $("e"));

		String sqlQuery = "SELECT * FROM T1 " +
							"UNION ALL " +
							"(SELECT a, b, c FROM T2 WHERE a	< 3)";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");
		expected.add("1,1,Hallo");
		expected.add("2,2,Hallo Welt");
		expected.add("2,3,Hallo Welt wie");

		StreamITCase.compareWithList(expected);
	}
}
