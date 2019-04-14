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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream._
import org.apache.flink.table.plan.nodes.decorate.{DecorateScanNode, DecorateSingleRelNode}

class ConvertCalcToDecorateRelNodeRule extends ConverterRule(
  classOf[DataStreamCalc],
  FlinkConventions.DATASTREAM,
  FlinkConventions.DECORATE,
  "ConvertCalcToDecorateRelNodeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel(0).asInstanceOf[DataStreamCalc]
    scan.inOutUpdateMode.isDefined
  }

  def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[DataStreamCalc]
    val traitSet = calc.getTraitSet.replace(FlinkConventions.DECORATE)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, FlinkConventions.DATASTREAM)

    new DecorateSingleRelNode(
      calc.getCluster,
      traitSet,
      convInput,
      calc
    )
  }
}

object ConvertCalcToDecorateRelNodeRule {
  val INSTANCE = new ConvertCalcToDecorateRelNodeRule
}




