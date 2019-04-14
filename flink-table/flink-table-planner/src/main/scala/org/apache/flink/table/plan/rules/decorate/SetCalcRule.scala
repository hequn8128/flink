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

package org.apache.flink.table.plan.rules.decorate

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.datastream.{DataStreamCalc, DataStreamRel}
import org.apache.flink.table.plan.nodes.decorate.DecorateRel

class SetCalcRule extends RelOptRule(
  operand(
    classOf[DataStreamCalc],
    operand(classOf[RelNode], none())),
  "SetCalcRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rel(0).asInstanceOf[DataStreamCalc]
    val input = call.rel(1).asInstanceOf[RelNode]

    val inputInOutUpdateMode = input match {
      case datastreamRel: DataStreamRel => datastreamRel.getDecidedInputOutputMode
      case decorateRel: DecorateRel => decorateRel.getInnerNode.getDecidedInputOutputMode
    }
    inputInOutUpdateMode.isDefined && calc.getDecidedInputOutputMode.isEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc = call.rel(0).asInstanceOf[DataStreamCalc]
    val input = call.rel(1).asInstanceOf[RelNode]
    val inputInOutUpdateMode = input match {
      case datastreamRel: DataStreamRel => datastreamRel.getDecidedInputOutputMode
      case decorateRel: DecorateRel => decorateRel.getInnerNode.getDecidedInputOutputMode
    }

    val traitSet = calc.getTraitSet
    val newRel = calc.copy(traitSet, input, calc.getProgram, inputInOutUpdateMode)
    call.transformTo(newRel)
  }
}

object SetCalcRule {
  val INSTANCE = new SetCalcRule
}



