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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

/**
 * Similar to {@link SqlAvgAggFunction}. The difference is {@link SqlWindowAvgAggFunction} contains
 * a self defined {@link SqlReturnTypeInference}, e.g, WINDOW_AVG_AGG_FUNCTION, which will not
 * change return type to nullable even if the call occurs within a "GROUP BY ()".
 *
 * <p>See more details in {@link ReturnTypes#AVG_AGG_FUNCTION}.
 */
public class SqlWindowAvgAggFunction extends SqlAggFunction {

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a SqlWindowAvgAggFunction.
	 */
	public SqlWindowAvgAggFunction() {
		super("WINDOW_AVG",
			null,
			SqlKind.AVG,
			WINDOW_AVG_AGG_FUNCTION,
			null,
			OperandTypes.NUMERIC,
			SqlFunctionCategory.NUMERIC,
			false,
			false,
			Optionality.FORBIDDEN);
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Returns the specific function, e.g. AVG.
	 *
	 * @return Subtype
	 */
	@Deprecated // to be removed before 2.0
	public Subtype getSubtype() {
		return Subtype.valueOf(kind.name());
	}

	/** Sub-type of aggregate function. */
	@Deprecated // to be removed before 2.0
	public enum Subtype {
		AVG,
		STDDEV_POP,
		STDDEV_SAMP,
		VAR_POP,
		VAR_SAMP
	}

	private static final SqlReturnTypeInference WINDOW_AVG_AGG_FUNCTION = opBinding -> {
		RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
		return typeFactory.getTypeSystem().deriveAvgAggType(typeFactory,
			opBinding.getOperandType(0));
	};
}

// End SqlWindowAvgAggFunction.java
