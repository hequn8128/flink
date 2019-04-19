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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.TableAggFunctionCall;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

/**
 * Utility class for creating a valid {@link TableAggFunctionCall} operation.
 */
@Internal
public class TableAggFunctionCallFactory {

	private final ExpressionBridge<PlannerExpression> bridge;

	public TableAggFunctionCallFactory(ExpressionBridge<PlannerExpression> bridge) {
		this.bridge = bridge;
	}

	/**
	 * Creates a valid {@link TableAggFunctionCall} operation.
	 *
	 * @param callExpr call to table aggregate function as expression
	 * @return valid calculated table
	 */
	public TableAggFunctionCall create(Expression callExpr) {
		return callExpr.accept(new TableAggFunctionCallVisitor());
	}

	private class TableAggFunctionCallVisitor extends ApiExpressionDefaultVisitor<TableAggFunctionCall> {

		@Override
		public TableAggFunctionCall visitCall(CallExpression call) {
			FunctionDefinition definition = call.getFunctionDefinition();
			if (definition.equals(AS)) {
				return unwrapFromAlias(call);
			} else if (definition instanceof AggregateFunctionDefinition) {
				return createTableAggFunctionCall(
					(AggregateFunctionDefinition) definition,
					Collections.emptyList(),
					call.getChildren());
			} else {
				return defaultMethod(call);
			}
		}

		private TableAggFunctionCall unwrapFromAlias(CallExpression call) {
			List<Expression> children = call.getChildren();
			List<String> aliases = children.subList(1, children.size())
				.stream()
				.map(alias -> ApiExpressionUtils.extractValue(alias, Types.STRING)
					.orElseThrow(() -> new ValidationException("Unexpected alias: " + alias)))
				.collect(toList());

			if (!isTableAggFunctionCall(children.get(0))) {
				throw fail();
			}

			CallExpression tableAggCall = (CallExpression) children.get(0);
			AggregateFunctionDefinition aggFunctionDefinition =
				(AggregateFunctionDefinition) tableAggCall.getFunctionDefinition();
			return createTableAggFunctionCall(aggFunctionDefinition, aliases, tableAggCall.getChildren());
		}

		private TableAggFunctionCall createTableAggFunctionCall(
				AggregateFunctionDefinition aggFunctionDefinition,
				List<String> aliases,
				List<Expression> parameters) {
			TypeInformation resultType = aggFunctionDefinition.getResultTypeInfo();

			int callArity = resultType.getTotalFields();
			int aliasesSize = aliases.size();

			if (aliasesSize > 0 && aliasesSize != callArity) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					aggFunctionDefinition.getName(),
					callArity,
					aliasesSize));
			}

			return new TableAggFunctionCall(
				(TableAggregateFunction) aggFunctionDefinition.getAggregateFunction(),
				aggFunctionDefinition.getResultTypeInfo(),
				aggFunctionDefinition.getAccumulatorTypeInfo(),
				parameters.stream().map(bridge::bridge).collect(toList()),
				aliases);
		}

		boolean isTableAggFunctionCall(Expression expression) {
			return expression instanceof CallExpression &&
				((CallExpression) expression).getFunctionDefinition().getType() == AGGREGATE_FUNCTION &&
				(((CallExpression) expression).getFunctionDefinition() instanceof AggregateFunctionDefinition) &&
				((AggregateFunctionDefinition) ((CallExpression) expression).getFunctionDefinition())
					.getAggregateFunction() instanceof TableAggregateFunction;
		}

		@Override
		protected TableAggFunctionCall defaultMethod(Expression expression) {
			throw fail();
		}

		private ValidationException fail() {
			return new ValidationException(
				"A flatAggregate only accepts an expression which defines a table aggregate " +
					"function that might be followed by some alias.");
		}
	}
}
