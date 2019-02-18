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

import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import java.lang.{Boolean => JBool}

import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.runtime.conversion.{ExternalTypeToCRowProcessRunner, JavaTupleToCRowProcessRunner, ScalaTupleToCRowProcessRunner}

trait StreamScan extends CommonScan[CRow] with DataStreamRel {

  protected def convertAppendToInternalRow(
    schema: RowSchema,
    input: DataStream[Any],
    fieldIndexes: Array[Int],
    config: TableConfig,
    rowtimeExpression: Option[RexNode]): DataStream[CRow] = {

    def getRow(v: Row): Row = v
    def getChange(v: Row): Boolean = true

    commonConvertToInternalRow[Any, Row](
      schema,
      input,
      fieldIndexes,
      config,
      input.getType,
      input.getType,
      rowtimeExpression,
      classOf[ExternalTypeToCRowProcessRunner],
      "AppendStreamSourceConversion",
      getRow,
      getChange
    )
  }

  protected def convertUpsertToInternalRow(
      schema: RowSchema,
      input: DataStream[Any],
      fieldIndexes: Array[Int],
      config: TableConfig,
      rowtimeExpression: Option[RexNode]): DataStream[CRow] = {

    val dsType = input.getType
    dsType match {
      // Scala tuple
      case t: CaseClassTypeInfo[_]
        if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>

        def getRow(v: (Boolean, Row)): Row = v._2
        def getChange(v: (Boolean, Row)): Boolean = v._1

        commonConvertToInternalRow[(Boolean, Any), (Boolean, Row)](
          schema,
          input,
          fieldIndexes,
          config,
          dsType,
          t.getTypeAt[Any](1),
          rowtimeExpression,
          classOf[ScalaTupleToCRowProcessRunner],
          "UpsertStreamSourceConversion",
          getRow,
          getChange
        )

      // Java tuple
      case t: TupleTypeInfo[_]
        if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>

        def getRow(v: JTuple2[JBool, Row]): Row = v.f1
        def getChange(v: JTuple2[JBool, Row]): Boolean = v.f0

        commonConvertToInternalRow[JTuple2[JBool, Any], JTuple2[JBool, Row]](
          schema,
          input,
          fieldIndexes,
          config,
          dsType,
          t.getTypeAt[Any](1),
          rowtimeExpression,
          classOf[JavaTupleToCRowProcessRunner],
          "UpsertStreamSourceConversion",
          getRow,
          getChange
        )
    }
  }

  /**
    * Common method to convert input to Internal Row. This method can be used to convert append
    * streams or upsert streams with tuple input type.
    *
    * @param schema              The schema of the logical or physical row.
    * @param input               The ingest datastream
    * @param fieldIndexes        The indexes of the field
    * @param config              The tableConfig
    * @param inputType           The type of the input datastream
    * @param dataType            The type of the data part. For upsert streams, it is the type of
    *                            The tuple type at position of 1.
    * @param rowtimeExpression   The rowtime expression to extract timestamp
    * @param convertClass        The convert class which used to convert input stream to CRow.
    * @param convertOperatorName The name for the code-generated operator.
    * @param getRow              The method to extract row.
    * @param getChange           The method to extract change.
    * @tparam D The type of datastream
    * @tparam S The type of the expected datastream contains Row.
    * @return The converted DataStream of type [[CRow]]
    */
  private def commonConvertToInternalRow[D, S](
      schema: RowSchema,
      input: DataStream[Any],
      fieldIndexes: Array[Int],
      config: TableConfig,
      inputType: TypeInformation[Any],
      dataType: TypeInformation[Any],
      rowtimeExpression: Option[RexNode],
      convertClass: Class[_],
      convertOperatorName: String,
      getRow: (S) => Row,
      getChange: (S) => Boolean): DataStream[CRow] = {

    val internalType = schema.typeInfo
    val cRowType = CRowTypeInfo(internalType)

    val hasTimeIndicator = fieldIndexes.exists(fieldIndex =>
      fieldIndex == TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER ||
        fieldIndex == TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER)

    if (inputType == cRowType && !hasTimeIndicator) {
      // input is already a CRow with correct type
      input.asInstanceOf[DataStream[CRow]]

    } else if (dataType == internalType && !hasTimeIndicator) {
      // input is already of correct type. Only need to wrap it as CRow
      input.asInstanceOf[DataStream[S]].map(new RichMapFunction[S, CRow] {
        @transient private var outCRow: CRow = _
        override def open(parameters: Configuration): Unit = {
          outCRow = new CRow(null, change = true)
        }

        override def map(v: S): CRow = {
          outCRow.row = getRow(v)
          outCRow.change = getChange(v)
          outCRow
        }
      }).returns(cRowType)

    } else {
      // input needs to be converted and wrapped as CRow or time indicators need to be generated

      val function = generateConversionProcessFunction(
        config,
        dataType,
        internalType,
        convertOperatorName,
        schema.fieldNames,
        fieldIndexes,
        rowtimeExpression
      )

      val processFunc = convertClass
        .getConstructor(classOf[String], classOf[String], classOf[TypeInformation[CRow]])
        .newInstance(function.name, function.code, cRowType)
        .asInstanceOf[ProcessFunction[D, CRow]]

      val opName = s"from: (${schema.fieldNames.mkString(", ")})"

      input
        .asInstanceOf[DataStream[D]]
        .process(processFunc).name(opName).returns(cRowType)
    }
  }

  private def generateConversionProcessFunction[D](
      config: TableConfig,
      inputType: TypeInformation[D],
      outputType: TypeInformation[Row],
      conversionOperatorName: String,
      fieldNames: Seq[String],
      inputFieldMapping: Array[Int],
      rowtimeExpression: Option[RexNode]): GeneratedFunction[ProcessFunction[Any, Row], Row] = {

    val generator = new FunctionCodeGenerator(
      config,
      false,
      inputType,
      None,
      Some(inputFieldMapping))

    val conversion = generator.generateConverterResultExpression(
      outputType,
      fieldNames,
      rowtimeExpression)

    val body =
      s"""
         |${conversion.code}
         |${generator.collectorTerm}.collect(${conversion.resultTerm});
         |""".stripMargin

    generator.generateFunction(
      "DataStreamSourceConversion",
      classOf[ProcessFunction[Any, Row]],
      body,
      outputType)
  }
}
