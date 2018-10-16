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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.{DataStreamLastRow, UpsertStreamScan}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalNativeTableScan
import org.apache.flink.table.plan.schema.{RowSchema, UpsertStreamTable}

import scala.collection.JavaConversions._

class DataStreamLastRowRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalNativeTableScan], any()),
    "DataStreamLastRowRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel(0).asInstanceOf[FlinkLogicalNativeTableScan]
    val upsertStreamTable = scan.getTable.unwrap(classOf[UpsertStreamTable[Any]])
    upsertStreamTable match {
      case _: UpsertStreamTable[Any] =>
        true
      case _ =>
        false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val scan = call.rel(0).asInstanceOf[FlinkLogicalNativeTableScan]
    val traitSet: RelTraitSet = scan.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val inputSchema = new RowSchema(scan.getRowType)

    // get unique key indexes
    val keyNames = scan.getTable.unwrap(classOf[UpsertStreamTable[_]]).uniqueKeys
    val keyIndexes = scan.getRowType.getFieldNames.zipWithIndex
      .filter(e => keyNames.contains(e._1))
      .map(_._2).toArray

    val upsertStreamScan = new UpsertStreamScan(
      scan.getCluster, traitSet, scan.getTable, inputSchema)
    val dataStreamLastRow = new DataStreamLastRow(
      scan.getCluster, traitSet, upsertStreamScan, inputSchema, inputSchema, keyIndexes)
    val relBuilder = call.builder()
    relBuilder.push(dataStreamLastRow)
    call.transformTo(relBuilder.build())
  }
}

object DataStreamLastRowRule {
  val INSTANCE = new DataStreamLastRowRule
}
