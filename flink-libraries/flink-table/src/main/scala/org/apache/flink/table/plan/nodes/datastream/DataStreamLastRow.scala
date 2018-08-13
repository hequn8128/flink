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

package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.{CRowKeySelector, LastRowProcessFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

class DataStreamLastRow(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   input: RelNode,
   inputSchema: RowSchema,
   schema: RowSchema,
   val uniqueKeys: Array[Int])
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel{

  override def needsUpdatesAsRetraction: Boolean = true

  override def producesUpdates: Boolean = true

  override def consumesRetractions: Boolean = true

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamLastRow(cluster, traitSet, inputs.get(0), inputSchema, schema, uniqueKeys)
  }
  // todo hequn generate operator name

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig

    val inputDS =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val outRowType = CRowTypeInfo(schema.typeInfo)

    val processFunction = new LastRowProcessFunction(
      new RowTypeInfo(schema.fieldTypeInfos.toArray, schema.fieldNames.toArray),
      DataStreamRetractionRules.isAccRetract(this),
      queryConfig
    )

    val result: DataStream[CRow] =
      if (uniqueKeys.nonEmpty) {
        // upsert with keys
        inputDS
          .keyBy((new CRowKeySelector(uniqueKeys, inputSchema.projectedTypeInfo(uniqueKeys))))
          .process(processFunction)
          .returns(outRowType)
          .name("DataStreamLastRow")
          .asInstanceOf[DataStream[CRow]]
      } else {
        // upsert without key -> single row table
        inputDS
          .keyBy(new NullByteKeySelector[CRow])
          .process(processFunction)
          .setParallelism(1)
          .setMaxParallelism(1)
          .returns(outRowType)
          .name("DataStreamLastRow")
          .asInstanceOf[DataStream[CRow]]
      }

    result
  }
}
