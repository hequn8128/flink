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

class ConvertToDecorateRelNodeRule extends ConverterRule(
  classOf[DataStreamRel],
  FlinkConventions.DATASTREAM,
  FlinkConventions.DECORATE,
  "ConvertToDecorateRelNodeRule") {

  // only convert [[DataStreamRel]] that the input output mode are decided.
  override def matches(call: RelOptRuleCall): Boolean = {
    val dataStreamRel = call.rel(0).asInstanceOf[DataStreamRel]

    val traitSet = dataStreamRel.getTraitSet
    val inputOutputUpdateModeTrait = traitSet.getTrait(InputOutputUpdateModeTraitDef.INSTANCE)
    inputOutputUpdateModeTrait.getInputUpdateMode == UpdateMode
  }

  def convert(rel: RelNode): RelNode = {
    val dataStreamRel = rel.asInstanceOf[DataStreamRel]
    val traitSet = dataStreamRel.getTraitSet.replace(FlinkConventions.DECORATE)

    val inputs = dataStreamRel.getInputs
    if (inputs.size() == 0) {
      // source
      new DecorateScanNode(
        dataStreamRel.getCluster,
        traitSet,
        dataStreamRel.asInstanceOf[TableScan],
        dataStreamRel
      )
    } else {
      // todo: need to consider multi input
      val convInput: RelNode = RelOptRule.convert(dataStreamRel.getInput(0), FlinkConventions.DATASTREAM)

      new DecorateSingleRelNode(
        dataStreamRel.getCluster,
        traitSet,
        convInput,
        dataStreamRel
      )
    }
  }
}

object ConvertToDecorateRelNodeRule {
  val INSTANCE = new ConvertToDecorateRelNodeRule
}


