/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Similar to {@link SqlSumAggFunction}. The difference is {@link SqlWindowSumAggFunction} contains
 * a self defined {@link SqlReturnTypeInference}, e.g, WINDOW_AGG_SUM, which will not change return
 * type to nullable even if the call occurs within a "GROUP BY ()".
 *
 * <p>See more details in {@link ReturnTypes#AGG_SUM}.
 */
public class SqlWindowSumAggFunction extends SqlAggFunction {

	//~ Instance fields --------------------------------------------------------

	@Deprecated // to be removed before 2.0
	private final RelDataType type;

	//~ Constructors -----------------------------------------------------------

	public SqlWindowSumAggFunction(RelDataType type) {
		super(
			"WINDOW_SUM",
			null,
			SqlKind.SUM,
			WINDOW_AGG_SUM,
			null,
			OperandTypes.NUMERIC,
			SqlFunctionCategory.NUMERIC,
			false,
			false,
			Optionality.FORBIDDEN);
		this.type = type;
	}

	//~ Methods ----------------------------------------------------------------

	@SuppressWarnings("deprecation")
	public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
		return ImmutableList.of(type);
	}

	@Deprecated // to be removed before 2.0
	public RelDataType getType() {
		return type;
	}

	@SuppressWarnings("deprecation")
	public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
		return type;
	}

	@Override public <T> T unwrap(Class<T> clazz) {
		if (clazz == SqlSplittableAggFunction.class) {
			return clazz.cast(SqlSplittableAggFunction.SumSplitter.INSTANCE);
		}
		return super.unwrap(clazz);
	}

	private static final SqlReturnTypeInference WINDOW_AGG_SUM = opBinding -> {
		RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
		return typeFactory.getTypeSystem()
			.deriveSumType(typeFactory, opBinding.getOperandType(0));
	};
}

// End SqlWindowSumAggFunction.java
