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
 * A ConnectedTable is a Table which contains two different schemas, i.e., the data in the table
 * contains two different types. Type1 for the first input while type2 for the second input.
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
	 * Performs a flatMap operation on a connected table. The operation calls a
	 * {@link CoTableFunction#eval1} for each element of the first input and
	 * {@link CoTableFunction#eval2} for each element of the second input. Each CoTableFunction
	 * call can return any number of elements.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val coTableFunction = new CoTableFunction
	 *   connectedTable
	 *     .flatMap(coTableFunction('a, 'b)('c, 'd))
	 *     .select('x, 'y, 'z)
	 * }
	 * </pre>
	 */
	Table flatMap(Expression coScalarFunction);

	/**
	 * Performs an aggregate operation with an aggregate function. You have to close the
	 * {@link #aggregate(Expression)} with a select statement. The output will be flattened if the
	 * output type is a Expression type.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc = new MyCoTableAggregateFunction
	 *   connectedTable
	 *     .aggregate(tableAggFunc('a)('b) as ('x, 'y, 'z))
	 *     .select('key, 'x, 'y, 'z)
	 * }
	 * </pre>
	 */
	AggregatedTable aggregate(Expression tableAggregateFunction);

	/**
	 * Performs a flatAggregate operation on a non-grouped connected table. FlatAggregate takes a
	 * CoTableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc = new MyCoTableAggregateFunction
	 *   connectedTable
	 *     .flatAggregate(tableAggFunc('a)('b) as ('x, 'y, 'z))
	 *     .select('x, 'y, 'z)
	 * }
	 * </pre>
	 */
	FlatAggregateTable flatAggregate(Expression tableAggregateFunction);
}
