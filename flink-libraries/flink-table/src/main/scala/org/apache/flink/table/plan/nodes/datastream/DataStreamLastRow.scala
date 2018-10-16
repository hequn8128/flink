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

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.{CRowKeySelector, ProcTimeLastRow, RowTimeLastRow}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConversions._

/**
  * Flink RelNode for ingesting upsert stream from source.
  */
class DataStreamLastRow(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   input: RelNode,
   inputSchema: RowSchema,
   schema: RowSchema,
   val indexes: Seq[Int])
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel{

  override def deriveRowType(): RelDataType = {
    // Change RowTime type to timestamp type, because unbounded Upsert can not output watermark,
    // i.e, upsert source may output(retract) records with a timestamp smaller than current
    // watermark. We only use rowtime to judge element order in Upsert source.
    val fieldTypeInfos = inputSchema.fieldTypeInfos
    val typeFactory = cluster.getTypeFactory
    if (fieldTypeInfos.exists(FlinkTypeFactory.isRowtimeIndicatorType(_))) {
      val typeInfos: Seq[TypeInformation[_]] = fieldTypeInfos.map(e => {
        e match {
          case _ if (FlinkTypeFactory.isRowtimeIndicatorType(e)) => SqlTimeTypeInfo.TIMESTAMP
          case _ => e
        }
      })
      val names = inputSchema.fieldNames
      val rowType = typeFactory.asInstanceOf[FlinkTypeFactory].buildLogicalRowType(names, typeInfos)
      typeFactory.builder.addAll(rowType.getFieldList).build()
    } else {
      // don't have to change row type if it is proctime
      inputSchema.relDataType
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    // take rowCnt and fieldCnt into account, so that cost will be smaller when generate LastRow
    // after Calc.
    planner.getCostFactory.makeCost(rowCnt, rowCnt * child.getRowType.getFieldCount, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamLastRow(cluster, traitSet, inputs.get(0), inputSchema, schema, indexes)
  }

  def getKeysString(): String = {
    val inFields = input.getRowType.getFieldNames.toArray
    if (indexes.nonEmpty) {
      s"${indexes.map(inFields(_)).mkString(", ")}"
    } else {
      "constant"
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter ={
    super.explainTerms(pw)
      .item("keys", getKeysString)
      .item("select", input.getRowType.getFieldNames.toArray.mkString(", "))
  }

  override def toString: String = {
    s"Last(Keys: (${getKeysString}), " +
      s"select: (${input.getRowType.getFieldNames.toArray.mkString(", ")}))"
  }

  override def producesUpdates: Boolean = true

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val inputDS =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val outRowType = CRowTypeInfo(schema.typeInfo)

    val needRetraction = DataStreamRetractionRules.isAccRetract(this)
    val rowtimeFieldIndex = schema.fieldTypeInfos.zipWithIndex
      .filter(e => FlinkTypeFactory.isRowtimeIndicatorType(e._1))
      .map(_._2)

    // The rowtime attribute can only be defined once in a table schema.
    if (rowtimeFieldIndex.size > 1) {
      throw new TableException("The number of Rowtime field should be less than 2. This is" +
        " a bug.")
    }

    val result: DataStream[CRow] = if (needRetraction || rowtimeFieldIndex.size > 0) {

      val rowTypeInfo = new RowSchema(getRowType).typeInfo.asInstanceOf[RowTypeInfo]
      val processFunction = if (rowtimeFieldIndex.size > 0) {
        new RowTimeLastRow(rowTypeInfo, queryConfig, rowtimeFieldIndex.head)
      } else {
        new ProcTimeLastRow(rowTypeInfo, queryConfig)
      }

      if (indexes.nonEmpty) {
        // upsert with keys
        inputDS
          .keyBy(
            new CRowKeySelector(indexes.toArray, inputSchema.projectedTypeInfo(indexes.toArray)))
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
    } else {
      // forward messages if doesn't generate retraction and without rowtime.
      inputDS
    }

    result
  }
}
