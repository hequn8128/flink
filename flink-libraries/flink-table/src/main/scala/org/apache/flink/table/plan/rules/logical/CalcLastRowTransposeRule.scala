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

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.{RexCall, RexInputRef}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalLastRow}

import scala.collection.JavaConversions._

/**
  * Use this rule to transpose Calc through LastRow. It is beneficial if we get smaller state size
  * in LastRow.
  */
class CalcLastRowTransposeRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[FlinkLogicalLastRow], none)),
  "CalcLastRowTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val lastRow = call.rel(1).asInstanceOf[FlinkLogicalLastRow]

    // column pruning or push Filter down
    calc.getRowType.getFieldCount <= lastRow.getRowType.getFieldCount &&
    // key fields should not be changed
      fieldsRemainAfterCalc(lastRow.keyNames, calc)
  }

  override def onMatch(call: RelOptRuleCall) {
    val calc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val lastRow = call.rel(1).asInstanceOf[FlinkLogicalLastRow]

    // get new calc
    val newCalc = calc.copy(calc.getTraitSet, lastRow.getInput, calc.getProgram)
    // get new lastRow
    val oldKeyNames = lastRow.keyNames
    val newKeyNames = getNamesAfterCalc(oldKeyNames, calc)

    val newLastRow = new FlinkLogicalLastRow(
      lastRow.getCluster,
      lastRow.getTraitSet,
      newCalc,
      newKeyNames)

    call.transformTo(newLastRow)
  }

  private def fieldsRemainAfterCalc(fields: Seq[String], calc: FlinkLogicalCalc): Boolean = {
    // get input output names
    val inOutNames = getInOutNames(calc)
    // contains all fields
    inOutNames.map(_._1).containsAll(fields)
  }

  /**
    * Get related output field names for input field names for Calc.
    */
  private def getInOutNames(calc: FlinkLogicalCalc): Seq[(String, String)] = {
    val inNames = calc.getInput.getRowType.getFieldNames
    calc.getProgram.getNamedProjects
      .map(p => {
        calc.getProgram.expandLocalRef(p.left) match {
          // output field is forwarded input field
          case r: RexInputRef => (r.getIndex, p.right)
          // output field is renamed input field
          case a: RexCall if a.getKind.equals(SqlKind.AS) =>
            a.getOperands.get(0) match {
              case ref: RexInputRef =>
                (ref.getIndex, p.right)
              case _ =>
                (-1, p.right)
            }
          // output field is not forwarded from input
          case _ => (-1, p.right)
        }
      })
      .filter(_._1 >= 0)
      .map(io => (inNames.get(io._1), io._2))
  }

  private def getNamesAfterCalc(names: Seq[String], calc: FlinkLogicalCalc): Seq[String] = {
    val inOutNames = getInOutNames(calc)
    inOutNames.filter(e => names.contains(e._1)).map(_._2)
  }
}

object CalcLastRowTransposeRule {
  val INSTANCE = new CalcLastRowTransposeRule()
}

