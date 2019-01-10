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

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.expressions.Cast
import org.apache.flink.table.plan.schema.{RowSchema, UpsertStreamTable}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

/**
  * Flink RelNode which matches along with DataStreamSource. Different from [[AppendStreamScan]],
  * [[UpsertStreamScan]] is used to handle upsert streams from source.
  */
class UpsertStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    schema: RowSchema)
  extends TableScan(cluster, traitSet, table)
  with StreamScan {

  val upsertStreamTable: UpsertStreamTable[Any] = getTable.unwrap(classOf[UpsertStreamTable[Any]])

  override def producesUpdates: Boolean = true

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new UpsertStreamScan(
      cluster,
      traitSet,
      getTable,
      schema
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = upsertStreamTable.dataStream
    val fieldIdxs = upsertStreamTable.fieldIndexes

    // get expression to extract timestamp
    val rowtimeExpr: Option[RexNode] =
      if (fieldIdxs.contains(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER)) {
        // extract timestamp from StreamRecord
        Some(
          Cast(
            org.apache.flink.table.expressions.StreamRecordTimestamp(),
            TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
            .toRexNode(tableEnv.getRelBuilder))
      } else {
        None
      }

    // convert DataStream
    convertUpsertToInternalRow(schema, inputDataStream, fieldIdxs, config, rowtimeExpr)
  }

}
