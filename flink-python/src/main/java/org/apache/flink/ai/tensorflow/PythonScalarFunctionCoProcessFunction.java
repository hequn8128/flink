///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.ai.tensorflow;
//
//import org.apache.flink.annotation.Internal;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeutils.TypeSerializer;
//import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.ConfigurationUtils;
//import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
//import org.apache.flink.core.memory.DataInputViewStreamWrapper;
//import org.apache.flink.python.PythonConfig;
//import org.apache.flink.python.PythonFunctionRunner;
//import org.apache.flink.python.PythonOptions;
//import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
//import org.apache.flink.python.env.PythonDependencyInfo;
//import org.apache.flink.python.env.PythonEnvironmentManager;
//import org.apache.flink.python.metric.FlinkMetricContainer;
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
//import org.apache.flink.table.functions.ScalarFunction;
//import org.apache.flink.table.functions.python.PythonEnv;
//import org.apache.flink.table.functions.python.PythonFunctionInfo;
//import org.apache.flink.table.runtime.runners.python.scalar.PythonScalarFunctionRunner;
//import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
//import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
//import org.apache.flink.table.types.utils.TypeConversions;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.Preconditions;
//
//import org.apache.beam.sdk.fn.data.FnDataReceiver;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.stream.Collectors;
//
///**
// * The {@link RichFlatMapFunction} used to invoke Python {@link ScalarFunction} functions for the
// * old planner.
// */
//@Internal
//public final class PythonScalarFunctionCoProcessFunction
//	extends CoProcessFunction<Row, Row, Row> implements ResultTypeQueryable<Row> {
//
//	private static final long serialVersionUID = 1L;
//
//	private static final Logger LOG = LoggerFactory.getLogger(PythonScalarFunctionCoProcessFunction.class);
//
//	/**
//	 * The python config.
//	 */
//	private final PythonConfig config;
//
//	/**
//	 * The offsets of user-defined function inputs.
//	 */
//	private final int[] userDefinedFunctionInputOffsets;
//
//	/**
//	 * The input logical type.
//	 */
//	protected final RowType inputType;
//
//	/**
//	 * The output logical type.
//	 */
//	protected final RowType outputType;
//
//	/**
//	 * The options used to configure the Python worker process.
//	 */
//	protected final Map<String, String> jobOptions;
//
//	/**
//	 * The user-defined function input logical type.
//	 */
//	protected transient RowType userDefinedFunctionInputType;
//
//	/**
//	 * The user-defined function output logical type.
//	 */
//	protected transient RowType userDefinedFunctionOutputType;
//
//	/**
//	 * The queue holding the input elements for which the execution results have not been received.
//	 */
//	protected transient LinkedBlockingQueue<Row> forwardedInputQueue;
//
//	/**
//	 * The queue holding the user-defined function execution results. The execution results
//	 * are in the same order as the input elements.
//	 */
//	protected transient LinkedBlockingQueue<byte[]> userDefinedFunctionResultQueue;
//
//	/**
//	 * Use an AtomicBoolean because we start/stop bundles by a timer thread.
//	 */
//	private transient AtomicBoolean bundleStarted;
//
//	/**
//	 * Max number of elements to include in a bundle.
//	 */
//	private transient int maxBundleSize;
//
//	/**
//	 * Max duration of a bundle.
//	 */
//	private transient long maxBundleTimeMills;
//
//	/**
//	 * A timer that finishes the current bundle after a fixed amount of time.
//	 */
//	private transient ScheduledFuture<?> checkFinishBundleTimer;
//
//	/**
//	 * The collector used to collect records.
//	 */
//	protected transient Collector<Row> resultCollector;
//
//	/**
//	 * Number of processed elements in the current bundle.
//	 */
//	private transient int elementCount;
//
//	/**
//	 * The {@link PythonFunctionRunner} which is responsible for Python user-defined function execution.
//	 */
//	private transient PythonFunctionRunner<Row> pythonFunctionRunner;
//
//	/**
//	 * Reusable InputStream used to holding the execution results to be deserialized.
//	 */
//	protected transient ByteArrayInputStreamWithPos bais;
//
//	/**
//	 * InputStream Wrapper.
//	 */
//	protected transient DataInputViewStreamWrapper baisWrapper;
//
//	/**
//	 * The TypeSerializer for user-defined function execution results.
//	 */
//	protected transient TypeSerializer<Row> userDefinedFunctionTypeSerializer;
//
//	/**
//	 * The type serializer for the forwarded fields.
//	 */
//	protected transient TypeSerializer<Row> forwardedInputSerializer;
//
//	/**
//	 * The Python {@link ScalarFunction}s to be executed.
//	 */
//	private final PythonFunctionInfo[] scalarFunctions;
//
//	/**
//	 * The offset of the fields which should be forwarded.
//	 */
//	private final int[] forwardedFields;
//
//	private Integer index;
//	private String serverPort;
//
//	public PythonScalarFunctionCoProcessFunction(
//		Configuration config,
//		PythonFunctionInfo[] scalarFunctions,
//		RowType inputType,
//		RowType outputType,
//		int[] userDefinedFunctionInputOffsets,
//		int[] forwardedFields) {
//		this.inputType = Preconditions.checkNotNull(inputType);
//		this.outputType = Preconditions.checkNotNull(outputType);
//		this.userDefinedFunctionInputOffsets = Preconditions.checkNotNull(userDefinedFunctionInputOffsets);
//		this.config = new PythonConfig(Preconditions.checkNotNull(config));
//		this.jobOptions = buildJobOptions(config);
//
//		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
//		this.forwardedFields = Preconditions.checkNotNull(forwardedFields);
//	}
//
//	@Override
//	public void open(Configuration parameters) throws Exception {
//		super.open(parameters);
//
//		index = null;
//		serverPort = null;
//
//		userDefinedFunctionOutputType = new RowType(outputType.getFields().subList(forwardedFields.length, outputType.getFieldCount()));
//
//		RowTypeInfo forwardedInputTypeInfo = new RowTypeInfo(
//			Arrays.stream(forwardedFields)
//				.mapToObj(i -> inputType.getFields().get(i))
//				.map(RowType.RowField::getType)
//				.map(TypeConversions::fromLogicalToDataType)
//				.map(TypeConversions::fromDataTypeToLegacyInfo)
//				.toArray(TypeInformation[]::new));
//		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
//
//		this.elementCount = 0;
//		this.bundleStarted = new AtomicBoolean(false);
//		this.maxBundleSize = config.getMaxBundleSize();
//		if (this.maxBundleSize <= 0) {
//			this.maxBundleSize = PythonOptions.MAX_BUNDLE_SIZE.defaultValue();
//			LOG.error("Invalid value for the maximum bundle size. Using default value of " +
//				this.maxBundleSize + '.');
//		} else {
//			LOG.info("The maximum bundle size is configured to {}.", this.maxBundleSize);
//		}
//
//		this.maxBundleTimeMills = config.getMaxBundleTimeMills();
//		if (this.maxBundleTimeMills <= 0L) {
//			this.maxBundleTimeMills = PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue();
//			LOG.error("Invalid value for the maximum bundle time. Using default value of " +
//				this.maxBundleTimeMills + '.');
//		} else {
//			LOG.info("The maximum bundle time is configured to {} milliseconds.", this.maxBundleTimeMills);
//		}
//
//		// Schedule timer to check timeout of finish bundle.
//		long bundleCheckPeriod = Math.max(this.maxBundleTimeMills, 1);
//		this.checkFinishBundleTimer =
//			getProcessingTimeService()
//				.scheduleAtFixedRate(
//					// ProcessingTimeService callbacks are executed under the checkpointing lock
//					timestamp -> checkInvokeFinishBundleByTime(), bundleCheckPeriod, bundleCheckPeriod);
//
//		forwardedInputQueue = new LinkedBlockingQueue<>();
//		userDefinedFunctionResultQueue = new LinkedBlockingQueue<>();
//		userDefinedFunctionInputType = new RowType(
//			Arrays.stream(userDefinedFunctionInputOffsets)
//				.mapToObj(i -> inputType.getFields().get(i))
//				.collect(Collectors.toList()));
//		bais = new ByteArrayInputStreamWithPos();
//		baisWrapper = new DataInputViewStreamWrapper(bais);
//
//		userDefinedFunctionTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
//
//		this.pythonFunctionRunner = createPythonFunctionRunner();
//		this.pythonFunctionRunner.open();
//	}
//
//	public void process(Row value, Collector<Row> out) throws Exception {
//		this.resultCollector = out;
//		bufferInput(value);
//
//		checkInvokeStartBundle();
//		pythonFunctionRunner.processElement(getFunctionInput(value));
//		checkInvokeFinishBundleByCount();
//		emitResults();
//	}
//
//	@Override
//	public TypeInformation<Row> getProducedType() {
//		return (TypeInformation<Row>) LegacyTypeInfoDataTypeConverter
//			.toLegacyTypeInfo(LogicalTypeDataTypeConverter.toDataType(outputType));
//	}
//
//	@Override
//	public void close() throws Exception {
//		try {
//			invokeFinishBundle();
//
//			if (pythonFunctionRunner != null) {
//				pythonFunctionRunner.close();
//				pythonFunctionRunner = null;
//			}
//		} finally {
//			super.close();
//		}
//	}
//
//	/**
//	 * Returns the {@link PythonEnv} used to create PythonEnvironmentManager..
//	 */
//	public PythonEnv getPythonEnv() {
//		return scalarFunctions[0].getPythonFunction().getPythonEnv();
//	}
//
//	public PythonFunctionRunner<Row> createPythonFunctionRunner() throws IOException {
//		FnDataReceiver<byte[]> userDefinedFunctionResultReceiver = input -> {
//			// handover to queue, do not block the result receiver thread
//			userDefinedFunctionResultQueue.put(input);
//		};
//
//		return new PythonScalarFunctionRunner(
//			getRuntimeContext().getTaskName(),
//			userDefinedFunctionResultReceiver,
//			scalarFunctions,
//			createPythonEnvironmentManager(),
//			userDefinedFunctionInputType,
//			userDefinedFunctionOutputType,
//			jobOptions,
//			getFlinkMetricContainer());
//	}
//
//	public void bufferInput(Row input) {
//		Row forwardedFieldsRow = Row.project(input, forwardedFields);
//		if (getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()) {
//			forwardedFieldsRow = forwardedInputSerializer.copy(forwardedFieldsRow);
//		}
//		forwardedInputQueue.add(forwardedFieldsRow);
//	}
//
//	public void emitResults() throws IOException {
//		byte[] rawUdfResult;
//		while ((rawUdfResult = userDefinedFunctionResultQueue.poll()) != null) {
//			Row input = forwardedInputQueue.poll();
//			bais.setBuffer(rawUdfResult, 0, rawUdfResult.length);
//			Row udfResult = userDefinedFunctionTypeSerializer.deserialize(baisWrapper);
//			this.resultCollector.collect(Row.join(input, udfResult));
//		}
//	}
//
//	protected PythonEnvironmentManager createPythonEnvironmentManager() throws IOException {
//		PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(
//			config, getRuntimeContext().getDistributedCache());
//		PythonEnv pythonEnv = getPythonEnv();
//		if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
//			return new ProcessPythonEnvironmentManager(
//				dependencyInfo,
//				ConfigurationUtils.splitPaths(System.getProperty("java.io.tmpdir")),
//				System.getenv());
//		} else {
//			throw new UnsupportedOperationException(String.format(
//				"Execution type '%s' is not supported.", pythonEnv.getExecType()));
//		}
//	}
//
//	protected FlinkMetricContainer getFlinkMetricContainer() {
//		return this.config.isMetricEnabled() ?
//			new FlinkMetricContainer(getRuntimeContext().getMetricGroup()) : null;
//	}
//
//	/**
//	 * Checks whether to invoke startBundle.
//	 */
//	private void checkInvokeStartBundle() throws Exception {
//		if (bundleStarted.compareAndSet(false, true)) {
//			pythonFunctionRunner.startBundle();
//		}
//	}
//
//	/**
//	 * Checks whether to invoke finishBundle by elements count. Called in flatMap.
//	 */
//	private void checkInvokeFinishBundleByCount() throws Exception {
//		elementCount++;
//		if (elementCount >= maxBundleSize) {
//			invokeFinishBundle();
//		}
//	}
//
//	private void invokeFinishBundle() throws Exception {
//		if (bundleStarted.compareAndSet(true, false)) {
//			pythonFunctionRunner.finishBundle();
//			emitResults();
//			elementCount = 0;
//		}
//	}
//
//	private Row getFunctionInput(Row element) {
//		return Row.project(element, userDefinedFunctionInputOffsets);
//	}
//
//	private Map<String, String> buildJobOptions(Configuration config) {
//		Map<String, String> jobOptions = new HashMap<>();
//		if (config.containsKey("table.exec.timezone")) {
//			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
//		}
//		return jobOptions;
//	}
//
//	@Override
//	public void processElement1(Row value, Context ctx, Collector<Row> out) throws Exception {
//		index = (Integer) value.getField(0);
//		if (index != null && serverPort != null) {
//			Row row = new Row(2);
//			row.setField(0, index);
//			row.setField(1, serverPort);
//			process(row, out);
//		}
//	}
//
//	@Override
//	public void processElement2(Row value, Context ctx, Collector<Row> out) throws Exception {
//		serverPort = (String) value.getField(0);
//		if (index != null && serverPort != null) {
//			Row row = new Row(2);
//			row.setField(0, index);
//			row.setField(1, serverPort);
//			process(row, out);
//		}
//	}
//}
