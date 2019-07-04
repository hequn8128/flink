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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been windowed and grouped for {@link GroupWindow}s.
 */
@PublicEvolving
public interface ConnectedTable {

	/**
	 * Performs a map operation on a connected table. The operation calls a
	 * {@link CoScalarFunction#eval1} for each element of the first input and
	 * {@link CoScalarFunction#eval2} for each element of the second input. Each CoScalarFunction
	 * call returns exactly one element.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val coScalarFunction = new CoScalarFunction
	 *   connectedTable
	 *     .map(coScalarFunction('a, 'b)('c, 'd))
	 *     .select('x, 'y, 'z)
	 * }
	 * </pre>
	 */
	Table map(Expression coScalarFunction);

	/**
	 * Performs a selection operation on a window grouped table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   windowGroupedTable.select('key, 'window.start, 'value.avg as 'valavg)
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);

	/**
	 * Performs a flatAggregate operation on a window grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   TableAggregateFunction tableAggFunc = new MyTableAggregateFunction
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   windowGroupedTable
	 *     .flatAggregate("tableAggFunc(a, b) as (x, y, z)")
	 *     .select("key, window.start, x, y, z")
	 * }
	 * </pre>
	 */
	FlatAggregateTable flatAggregate(String tableAggregateFunction);

	/**
	 * Performs a flatAggregate operation on a window grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc = new MyTableAggregateFunction
	 *   windowGroupedTable
	 *     .flatAggregate(tableAggFunc('a, 'b) as ('x, 'y, 'z))
	 *     .select('key, 'window.start, 'x, 'y, 'z)
	 * }
	 * </pre>
	 */
	FlatAggregateTable flatAggregate(Expression tableAggregateFunction);
}
