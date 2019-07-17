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

package org.apache.flink.table.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlWindowAvgAggFunction;
import org.apache.calcite.sql.fun.SqlWindowSumAggFunction;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Replace a {@link SqlSumAggFunction} to a {@link SqlWindowSumAggFunction} or replace a
 * {@link SqlAvgAggFunction} to a {@link SqlWindowAvgAggFunction} when it is a group window
 * aggregate.
 *
 * <p>This rule should be called before the window rules to replace the corresponding
 * {@link SqlAggFunction}.
 */
public class ReplaceWindowAggregateFunctionRule extends RelOptRule {

	public static final ReplaceWindowAggregateFunctionRule INSTANCE =
		new ReplaceWindowAggregateFunctionRule(
			operand(LogicalAggregate.class,
				operand(LogicalProject.class, any())),
			"ReplaceWindowAggregateFunctionRule");

	public ReplaceWindowAggregateFunctionRule(RelOptRuleOperand operand, String description) {
		super(operand, description);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalAggregate agg = call.rel(0);
		LogicalProject project = call.rel(1);
		return isWindowAggregate(agg, project);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalAggregate agg = call.rel(0);

		// Replace SqlReturnTypeInference for sum, avg and covar.
		List<AggregateCall> newAggCalls = replaceAggCalls(agg.getAggCallList());
		LogicalAggregate newAgg = LogicalAggregate.create(
			agg.getInput(),
			agg.getGroupSet(),
			agg.getGroupSets(),
			newAggCalls);

		call.transformTo(newAgg);
	}

	/**
	 * Replace a {@link SqlSumAggFunction} to a {@link SqlWindowSumAggFunction} or replace a
	 * {@link SqlAvgAggFunction} to a {@link SqlWindowAvgAggFunction}.
	 */
	private List<AggregateCall> replaceAggCalls(List<AggregateCall> origAggCalls) {
		List<AggregateCall> newAggCalls = new LinkedList<>();
		for (int i = 0; i < origAggCalls.size(); ++i) {
			AggregateCall aggregateCall = origAggCalls.get(i);
			// replace
			SqlAggFunction newSqlAggFunction = aggregateCall.getAggregation();
			if (aggregateCall.getAggregation().kind.equals(SqlKind.SUM)) {
				newSqlAggFunction = new SqlWindowSumAggFunction(aggregateCall.getType());
			} else if (aggregateCall.getAggregation().kind.equals(SqlKind.AVG)) {
				newSqlAggFunction = new SqlWindowAvgAggFunction();
			}
			AggregateCall newAggCall = AggregateCall.create(
				newSqlAggFunction,
				aggregateCall.isDistinct(),
				aggregateCall.isApproximate(),
				aggregateCall.ignoreNulls(),
				aggregateCall.getArgList(),
				aggregateCall.filterArg,
				aggregateCall.collation,
				aggregateCall.type,
				aggregateCall.name);
			newAggCalls.add(newAggCall);
		}
		return newAggCalls;
	}

	private boolean isWindowAggregate(LogicalAggregate agg, LogicalProject project) {
		List<Integer> groupKeys = agg.getGroupSet().toList();

		List<RexNode> groupExpr = new LinkedList<>();
		for (int i = 0; i < project.getProjects().size(); ++i) {
			if (groupKeys.contains(i)) {
				groupExpr.add(project.getProjects().get(i));
			}
		}

		List<RexCall> windowExprs = groupExpr.stream().filter(p -> p instanceof RexCall)
			.map(p -> (RexCall) p)
			.filter(p -> {
				if ((SqlKind.TUMBLE == p.getOperator().getKind()) ||
					(SqlKind.SESSION == p.getOperator().getKind())) {
					return 2 == p.getOperands().size();
				} else if (SqlKind.HOP == p.getOperator().getKind()) {
					return 3 == p.getOperands().size();
				} else {
					return false;
				}
			}).collect(Collectors.toList());

		return 1 == windowExprs.size();
	}
}
