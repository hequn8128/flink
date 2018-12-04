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
package org.apache.flink.table.codegen

import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.functions.UserDefinedAggregateFunction
import org.apache.flink.table.runtime.aggregate.GeneratedTableAggregations
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.RetractableCollector
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.table.codegen.TableAggregationCodeGenerator._
import org.apache.flink.table.plan.schema.RowSchema

/**
  * A base code generator for generating [[GeneratedTableAggregations]].
  *
  * @param config                 configuration that determines runtime behavior
  * @param nullableInput          input(s) can be null.
  * @param input                  type information about the input of the Function
  * @param constants              constant expressions that act like a second input in the
  *                               parameter indices.
  * @param name                   Class name of the function.
  *                               Does not need to be unique but has to be a valid Java class
  *                               identifier.
  * @param physicalInputTypes     Physical input row types
  * @param tableAggOutputTypes    Output types of TableAggregateFunction
  * @param outputSchema           The type of the rows emitted by TableAggregate operator
  * @param aggregates             All aggregate functions
  * @param aggFields              Indexes of the input fields for all aggregate functions
  * @param aggMapping             The mapping of aggregates to output fields
  * @param isDistinctAggs         The flag array indicating whether it is distinct aggregate.
  * @param isStateBackedDataViews a flag to indicate if distinct filter uses state backend.
  * @param partialResults         A flag defining whether final or partial results (accumulators)
  *                               are set
  *                               to the output row.
  * @param fwdMapping             The mapping of input fields to output fields
  * @param mergeMapping           An optional mapping to specify the accumulators to merge. If not
  *                               set, we
  *                               assume that both rows have the accumulators at the same position.
  * @param outputArity            The number of fields in the output row.
  * @param needRetract            a flag to indicate if the aggregate needs the retract method
  * @param needEmitWithRetract    a flag to indicate if the aggregate needs to output retractions
  *                               when update
  * @param needMerge              a flag to indicate if the aggregate needs the merge method
  * @param needReset              a flag to indicate if the aggregate needs the resetAccumulator
  *                               method
  * @param accConfig              Data view specification for accumulators
  */
class TableAggregationCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any],
    constants: Option[Seq[RexLiteral]],
    name: String,
    physicalInputTypes: Seq[TypeInformation[_]],
    tableAggOutputTypes: TypeInformation[_],
    outputSchema: RowSchema,
    aggregates: Array[UserDefinedAggregateFunction[_ <: Any, _ <: Any]],
    aggFields: Array[Array[Int]],
    aggMapping: Array[Int],
    isDistinctAggs: Array[Boolean],
    isStateBackedDataViews: Boolean,
    partialResults: Boolean,
    fwdMapping: Array[Int],
    mergeMapping: Option[Array[Int]],
    outputArity: Int,
    needRetract: Boolean,
    needEmitWithRetract: Boolean,
    needMerge: Boolean,
    needReset: Boolean,
    accConfig: Option[Array[Seq[DataViewSpec[_]]]])
  extends AggregationBaseCodeGenerator(
    config,
    nullableInput,
    input,
    constants,
    name,
    physicalInputTypes,
    aggregates,
    aggFields,
    aggMapping,
    isDistinctAggs,
    isStateBackedDataViews,
    partialResults,
    fwdMapping,
    mergeMapping,
    outputArity,
    needRetract,
    needMerge,
    needReset,
    accConfig) {


  def genEmit: String = {

    val sig: String =
      j"""
         |  public final void emit(
         |    $ROW accs,
         |    $RETRACTABLE_COLLECTOR<$ROW> collector) throws Exception """.stripMargin

    val emitMethodName = if (needEmitWithRetract) "emitValueWithRetract" else "emitValue"
    val emit: String = {
      for (i <- aggs.indices) yield {
        val emitAcc =
          j"""
             |      ${genAccDataViewFieldSetter(s"acc$i", i)}
             |      ${aggs(i)}.$emitMethodName(acc$i
             |        ${if (!parametersCode(i).isEmpty) "," else ""}
             |        ${CONVERT_COLLECTOR_VARIABLE_TERM});
               """.stripMargin
        j"""
           |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
           |    $CONVERT_COLLECTOR_VARIABLE_TERM.$COLLECTOR_VARIABLE_TERM = collector;
           |    $emitAcc
               """.stripMargin
      }
    }.mkString("\n")

    j"""$sig {
       |$emit
       |  }""".stripMargin
  }

  def genRecordToRow: String = {
    // gen access expr

    val functionGenerator = new FunctionCodeGenerator(
      config,
      false,
      tableAggOutputTypes,
      None,
      None,
      None)

    functionGenerator.outRecordTerm = s"$CONVERTER_ROW_RESULT_TERM"
    val inputAccessExprs = functionGenerator.generateFieldAccessExprs

    // gen result expr
    val conversion = functionGenerator.generateResultExpression(
      inputAccessExprs,
      outputSchema.typeInfo,
      outputSchema.fieldNames)
    conversion.code
  }

  override def genSetAggregationResults: String = {

    val sig: String =
      j"""
         |  public final void setAggregationResults(
         |    org.apache.flink.types.Row accs,
         |    org.apache.flink.types.Row output) throws Exception """.stripMargin

    val setAggs: String = {
      for (i <- aggs.indices) yield

        if (partialResults) {
          j"""
             |    output.setField(
             |      ${aggMapping(i)},
             |      (${accTypes(i)}) accs.getField($i));""".stripMargin
        } else {
          // we don't need set aggregate results for final agg function, i.e, use emit.
            j"""
               |
              """.stripMargin

        }
    }.mkString("\n")

    j"""
       |$sig {
       |$setAggs
       |  }""".stripMargin
  }

  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @param isBatch Whether generate class for batch.
    * @return A GeneratedAggregationsFunction
    */
  def generateTableAggregations: GeneratedTableAggregationsFunction = {

    init()
    val aggFuncCode = Seq(
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genMergeAccumulatorsPair,
      genResetAccumulator,
      genEmit,
      genSetForwardedFields,
      genSetAggregationResults,
      genCreateOutputRow).mkString("\n")

    val aggOutputTypeName = tableAggOutputTypes.getTypeClass.getCanonicalName
    val generatedAggregationsClass = classOf[GeneratedTableAggregations].getCanonicalName

    val funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  private $CONVERT_COLLECTOR_CLASS_TERM $CONVERT_COLLECTOR_VARIABLE_TERM;
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |    $CONVERT_COLLECTOR_VARIABLE_TERM = new $CONVERT_COLLECTOR_CLASS_TERM();
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |
         |  public class $CONVERT_COLLECTOR_CLASS_TERM extends $RETRACTABLE_COLLECTOR {
         |
         |      public $RETRACTABLE_COLLECTOR<$ROW> $COLLECTOR_VARIABLE_TERM;
         |      public final $ROW $CONVERTER_ROW_RESULT_TERM = new $ROW($outputArity);
         |
         |      public void convertToRow(Object record) throws Exception {
         |         $aggOutputTypeName in1 = ($aggOutputTypeName) record;
         |         ${genRecordToRow}
         |      }
         |
         |      @Override
         |      public void collect(Object record) throws Exception {
         |          convertToRow(record);
         |          $COLLECTOR_VARIABLE_TERM.collect($CONVERTER_ROW_RESULT_TERM);
         |      }
         |
         |      @Override
         |      public void retract(Object record) throws Exception {
         |          convertToRow(record);
         |          $COLLECTOR_VARIABLE_TERM.retract($CONVERTER_ROW_RESULT_TERM);
         |      }
         |
         |      @Override
         |      public void close() {
         |       $COLLECTOR_VARIABLE_TERM.close();
         |      }
         |  }
         |}
         """.stripMargin

    GeneratedTableAggregationsFunction(funcName, funcCode)
  }
}

object TableAggregationCodeGenerator {
  val CONVERT_COLLECTOR_CLASS_TERM = "ConvertCollector"
  val CONVERT_COLLECTOR_VARIABLE_TERM = "convertCollector"
  val COLLECTOR_VARIABLE_TERM = "collector"

  val CONVERTER_CROW_RESULT_TERM = "cRowTerm"
  val CONVERTER_ROW_RESULT_TERM = "rowTerm"

  val RETRACTABLE_COLLECTOR: String = classOf[RetractableCollector[_]].getCanonicalName
  val COLLECTOR: String = classOf[Collector[_]].getCanonicalName
  val CROW: String = classOf[CRow].getCanonicalName
  val ROW: String = classOf[Row].getCanonicalName
}
