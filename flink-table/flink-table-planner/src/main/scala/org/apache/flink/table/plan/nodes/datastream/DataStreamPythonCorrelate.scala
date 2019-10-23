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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.api.{StreamQueryConfig}
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo, SimplePythonFunction}
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.CommonCorrelate
import org.apache.flink.table.plan.nodes.datastream.DataStreamPythonCorrelate.PYTHON_TABLE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions

import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class DataStreamPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputSchema: RowSchema,
    input: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    schema: RowSchema,
    joinSchema: RowSchema,
    joinType: JoinRelType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with CommonCorrelate
  with DataStreamRel {

  override def deriveRowType() = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamPythonCorrelate(
      cluster,
      traitSet,
      inputSchema,
      inputs.get(0),
      scan,
      condition,
      schema,
      joinSchema,
      joinType,
      ruleDescription)
  }

  private lazy val pythonRexCall = scan.getCall.asInstanceOf[RexCall]

  private lazy val pythonUdtfInputOffsets = pythonRexCall
    .getOperands
    .collect { case rexInputRef: RexInputRef => rexInputRef.getIndex.asInstanceOf[Object] }
    .toArray

  override def toString: String = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    correlateToString(inputSchema.relDataType, rexCall, sqlFunction, getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("correlate", correlateToString(
        inputSchema.relDataType,
        rexCall, sqlFunction,
        getExpressionString))
      .item("select", selectToString(schema.relDataType))
      .item("rowType", schema.relDataType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)
    val inputParallelism = inputDataStream.getParallelism

    // Constructs the Python operator
    val correlateInputType = TypeConversions.fromLegacyInfoToDataType(
      inputSchema.typeInfo).getLogicalType.asInstanceOf[RowType]
    val correlateOutputType = TypeConversions.fromLegacyInfoToDataType(
      schema.typeInfo).getLogicalType.asInstanceOf[RowType]

    val pythonFunctionInfo = pythonRexCall.getOperator match {
      case tfc: TableSqlFunction =>
        val pythonFunction = new SimplePythonFunction(
          tfc.getTableFunction.asInstanceOf[PythonFunction].getSerializedPythonFunction,
          tfc.getTableFunction.asInstanceOf[PythonFunction].getPythonEnv)
        new PythonFunctionInfo(pythonFunction, pythonUdtfInputOffsets)
    }

    val udtfOperator = getPythonTableFunctionOperator(
      correlateInputType,
      correlateOutputType,
      pythonFunctionInfo)

    val sqlFunction = pythonRexCall.getOperator.asInstanceOf[TableSqlFunction]
    inputDataStream.transform(
      correlateOpName(
        inputSchema.relDataType,
        pythonRexCall,
        sqlFunction,
        schema.relDataType,
        getExpressionString),
      CRowTypeInfo(schema.typeInfo),
      udtfOperator
    ).setParallelism(inputParallelism)
  }

  private[flink] def getPythonTableFunctionOperator(
    inputRowType: RowType,
    outputRowType: RowType,
    pythonFunctionInfo: PythonFunctionInfo) = {
    val clazz = Class.forName(PYTHON_TABLE_FUNCTION_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      inputRowType,
      outputRowType,
      pythonFunctionInfo)
      .asInstanceOf[OneInputStreamOperator[CRow, CRow]]
  }
}

object DataStreamPythonCorrelate {
  val PYTHON_TABLE_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.PythonTableFunctionOperator"
}
