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
package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.core.AggregateCall
import org.apache.flink.table.functions.aggfunctions.LastValueNullableAggFunction
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.plan.nodes.datastream.{AccMode, DataStreamGroupAggregate}
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import _root_.org.apache.flink.table.plan.nodes.datastream.DataStreamCalc
import org.apache.calcite.rel.logical.{LogicalCalc, LogicalProject}
import java.util.{List => JList}

import org.apache.calcite.rex.{RexInputRef, RexProgram}
import org.apache.flink.table.plan.schema.RowSchema

import scala.collection.JavaConversions._

/**
  * Rule to remove [[DataStreamGroupAggregate]] if it is an UpsertToRetract Node and under
  * [[AccMode.Acc]] . In this case, it is a no-op node.
  */
class RemoveUpsertToRetractionRule extends RelOptRule(
  operand(
    classOf[DataStreamGroupAggregate], none()),
  "RemoveUpsertToRetractionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel(0).asInstanceOf[DataStreamGroupAggregate]
    RemoveUpsertToRetractionRule.isUpsertToRetraction(rel.getNamedAggregates)
  }

  private def getInputRefIndex(agg: DataStreamGroupAggregate): JList[Integer] = {
    agg.getGroupings.map(Integer.valueOf(_)).toList ++
      agg.getNamedAggregates.flatMap(_.left.getArgList)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel: DataStreamGroupAggregate = call.rel(0).asInstanceOf[DataStreamGroupAggregate]
    val input = rel.getInput.asInstanceOf[HepRelVertex].getCurrentRel

    if (!DataStreamRetractionRules.isAccRetract(rel)) {
      val inputRefIndexes = getInputRefIndex(rel)
      val inputRefs = inputRefIndexes.map(e => new RexInputRef(e, input.getRowType.getFieldList.get(e).getType))
      val rexProgram = RexProgram.create(
        input.getRowType,
        inputRefs,
        null,
        rel.getRowType,
        input.getCluster.getRexBuilder);
      val calc = new DataStreamCalc(
        rel.getCluster,
        rel.getTraitSet,
        input,
        new RowSchema(input.getRowType),
        new RowSchema(rel.getRowType),
        rexProgram,
        description
      )
      call.transformTo(calc)
    }
  }
}

object RemoveUpsertToRetractionRule {
  val INSTANCE: RemoveUpsertToRetractionRule = new RemoveUpsertToRetractionRule

  def isUpsertToRetraction(namedAggregates: Seq[CalcitePair[AggregateCall, String]]): Boolean = {
    namedAggregates.forall(t => t.left.getAggregation match {
      case aggSqlFunction: AggSqlFunction =>
        aggSqlFunction.getFunction match {
          case _: LastValueNullableAggFunction[_] => true
          case _ => false
        }
      case _ => false
    })
  }
}
