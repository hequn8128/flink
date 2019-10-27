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

package org.apache.flink.table.runtime.operators.python;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.PythonTableFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.stream.IntStream;

/**
 * The Python {@link TableFunction} operator for the legacy planner.
 */
@Internal
public class PythonTableFunctionOperator extends AbstractPythonTableFunctionOperator<CRow, CRow, Row, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordCRowWrappingCollector cRowWrapper;

	public PythonTableFunctionOperator(
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets) {
		super(tableFunction, inputType, outputType, udfInputOffsets);
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);
	}

	@Override
	public void bufferInput(CRow input) {
		forwardedInputQueue.add(input);
	}

	@Override
	public Row getUdtfInput(CRow element) {
		return Row.project(element.row(), udtfInputOffsets);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() {
		Row udtfResult;
		CRow left = null;
		// todo: more efficient
		while ((udtfResult = udtfResultQueue.poll()) != null) {
			// if it is the start row
			if ((long) udtfResult.getField(udtfOutputType.getFieldCount() - 1) == 1) {
				left = forwardedInputQueue.poll();
			}
			cRowWrapper.setChange(left.change());
			cRowWrapper.collect(Row.join(
				left.row(),
				Row.project(udtfResult, IntStream.range(0, udtfOutputType.getFieldCount() - 1).toArray())));
		}
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner(FnDataReceiver<Row> resultReceiver) {
		return new PythonTableFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			tableFunction,
			tableFunction.getPythonFunction().getPythonEnv(),
			udtfInputType,
			udtfOutputType,
			getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories());
	}

	/**
	 * The collector is used to convert a {@link Row} to a {@link CRow}.
	 */
	private static class StreamRecordCRowWrappingCollector implements Collector<Row> {

		private final Collector<StreamRecord<CRow>> out;
		private final CRow reuseCRow = new CRow();

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<CRow> reuseStreamRecord = new StreamRecord<>(reuseCRow);

		StreamRecordCRowWrappingCollector(Collector<StreamRecord<CRow>> out) {
			this.out = out;
		}

		public void setChange(boolean change) {
			this.reuseCRow.change_$eq(change);
		}

		@Override
		public void collect(Row record) {
			reuseCRow.row_$eq(record);
			out.collect(reuseStreamRecord);
		}

		@Override
		public void close() {
			out.close();
		}
	}
}
