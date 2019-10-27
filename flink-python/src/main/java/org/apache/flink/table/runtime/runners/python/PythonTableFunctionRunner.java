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

package org.apache.flink.table.runtime.runners.python;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/**
 * A {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 * It takes {@link Row} as the input and output type.
 */
@Internal
public class PythonTableFunctionRunner extends AbstractPythonTableFunctionRunner<Row, Row> {

	public PythonTableFunctionRunner(
		String taskName,
		FnDataReceiver<Row> resultReceiver,
		PythonFunctionInfo tableFunction,
		PythonEnv pythonEnv,
		RowType inputType,
		RowType outputType,
		String[] tempDirs) {
		super(taskName, resultReceiver, tableFunction, pythonEnv, inputType, outputType, tempDirs);
	}

	@Override
	public RowSerializer getInputTypeSerializer() {
		return (RowSerializer) PythonTypeUtils.toFlinkTypeSerializer(getInputType());
	}

	@Override
	public RowSerializer getOutputTypeSerializer() {
		return (RowSerializer) PythonTypeUtils.toFlinkTypeSerializer(getOutputType());
	}
}
