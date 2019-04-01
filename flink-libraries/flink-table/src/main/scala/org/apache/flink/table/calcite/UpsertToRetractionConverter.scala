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

package org.apache.flink.table.calcite

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode}
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.{LastValueNullableAggFunction, _}
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.plan.schema.UpsertStreamTable

import scala.collection.JavaConversions._

/**
  * Traverses a [[RelNode]] tree and add [[LogicalAggregate]] node after the upsert source. The
  * [[LogicalAggregate]] node is used to convert upsert to retractions.
  */
class UpsertToRetractionConverter(rexBuilder: RexBuilder) extends RelHomogeneousShuttle {

  override def visit(other: RelNode): RelNode = {
    other match {
      case scan: TableScan => {
        scan.getTable.unwrap(classOf[UpsertStreamTable[_]]) match {
          case upsertStreamTable: UpsertStreamTable[_] =>

            // 1. add a project to put key fields into the front of the row.
            val newNames = upsertStreamTable.uniqueKeys ++
              upsertStreamTable.fieldNames.filter(!upsertStreamTable.uniqueKeys.contains(_))
            val putKeyToFrontProject = getProject(scan, newNames)

            // 2. add aggregate node and perform last value
            val upsertToRetractionAgg = getUpsertToRetractionAggregate(
              putKeyToFrontProject,
              upsertStreamTable.uniqueKeys)

            // 3. add project to recover field position
            getProject(upsertToRetractionAgg, scan.getRowType.getFieldNames)

          case _ => scan
        }
      }
      case _ => other.copy(other.getTraitSet, other.getInputs.map(_.accept(this)))
    }
  }

  private def getProject(input: RelNode, toNames: Seq[String]): LogicalProject = {
    val fromNames = input.getRowType.getFieldNames
    val inputRefs = toNames
      .map(fromNames.indexOf(_))
      .map(e => new RexInputRef(e, input.getRowType.getFieldList.get(e).getType))
    LogicalProject.create(
      input,
      inputRefs.toList,
      toNames
    )
  }

  private def getUpsertToRetractionAggregate(
    inputProject: LogicalProject,
    keyNames: Array[String]): LogicalAggregate = {

    val groupSet = ImmutableBitSet.range(keyNames.size)
    val inputFieldList = inputProject.getRowType.getFieldList

    val aggCalls: util.List[AggregateCall] =
      (keyNames.size until inputFieldList.size()).map { index =>

        val fieldRelDataType = inputFieldList.get(index).getType
        val aggSqlFunction = getAggSqlFunctionForLastValue(
          fieldRelDataType,
          inputProject.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

        AggregateCall.create(
          aggSqlFunction,
          false,
          false,
          Array(Integer.valueOf(index)).toList,
          -1,
          fieldRelDataType,
          inputFieldList.get(index).getName
        )
      }

    LogicalAggregate.create(
      inputProject,
      groupSet,
      ImmutableList.of(groupSet),
      aggCalls
    )
  }

  def getAggSqlFunctionForLastValue(
    fieldRelDataType: RelDataType,
    flinkTypeFactory: FlinkTypeFactory): AggSqlFunction = {

    val aggregateFunction: AggregateFunction[_, _] =
      new LastValueNullableAggFunction(FlinkTypeFactory.toTypeInfo(fieldRelDataType))

    AggSqlFunction(
      aggregateFunction.functionIdentifier(),
      aggregateFunction.toString,
      aggregateFunction,
      FlinkTypeFactory.toTypeInfo(fieldRelDataType),
      aggregateFunction.getAccumulatorType,
      flinkTypeFactory,
      false
    )
  }
}

object UpsertToRetractionConverter {

  def convert(rootRel: RelNode, rexBuilder: RexBuilder): RelNode = {
    val converter = new UpsertToRetractionConverter(rexBuilder)
    rootRel.accept(converter)
  }
}
