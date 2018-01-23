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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexNode, RexProgram, RexProgramBuilder}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamCorrelate
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamCorrelateRule
  extends ConverterRule(
    classOf[FlinkLogicalCorrelate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamCorrelateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    // find only calc and table function
    def findTableFunction(calc: FlinkLogicalCalc): Boolean = {
      val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
      child match {
        case scan: FlinkLogicalTableFunctionScan => true
        case calc: FlinkLogicalCalc => findTableFunction(calc)
        case _ => false
      }
    }

    right match {
      // right node is a table function
      case scan: FlinkLogicalTableFunctionScan => true
      // one or more filters are pushed above the table function
      case calc: FlinkLogicalCalc => findTableFunction(calc)
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalCorrelate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM)
    val right: RelNode = join.getInput(1)

    def getTableScan(calc: FlinkLogicalCalc): RelNode = {
      val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
      child match {
        case scan: FlinkLogicalTableFunctionScan => scan
        case calc: FlinkLogicalCalc => getTableScan(calc)
        case _ => throw TableException("This must be a bug, could not find table scan")
      }
    }

    def getMergedCalc(calc: FlinkLogicalCalc): FlinkLogicalCalc = {
      val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
      if (child.isInstanceOf[FlinkLogicalCalc]) {
        val bottomCalc = getMergedCalc(child.asInstanceOf[FlinkLogicalCalc])
        val topCalc = calc
        val topProgram: RexProgram = topCalc.getProgram
        val mergedProgram: RexProgram = RexProgramBuilder
          .mergePrograms(
            topCalc.getProgram,
            bottomCalc.getProgram,
            topCalc.getCluster.getRexBuilder)
        assert(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)
        topCalc
          .copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
          .asInstanceOf[FlinkLogicalCalc]
      } else {
        calc
      }
    }

    def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): DataStreamCorrelate = {
      relNode match {
        case rel: RelSubset =>
          convertToCorrelate(rel.getRelList.get(0), condition)

        case calc: FlinkLogicalCalc =>
          val tableScan = getTableScan(calc)
          val newCalc = getMergedCalc(calc)
          convertToCorrelate(
            tableScan,
            Some(newCalc.getProgram.expandLocalRef(newCalc.getProgram.getCondition)))

        case scan: FlinkLogicalTableFunctionScan =>
          new DataStreamCorrelate(
            rel.getCluster,
            traitSet,
            new RowSchema(convInput.getRowType),
            convInput,
            scan,
            condition,
            new RowSchema(rel.getRowType),
            new RowSchema(join.getRowType),
            join.getJoinType,
            description)
      }
    }
    convertToCorrelate(right, None)
  }

}

object DataStreamCorrelateRule {
  val INSTANCE: RelOptRule = new DataStreamCorrelateRule
}
