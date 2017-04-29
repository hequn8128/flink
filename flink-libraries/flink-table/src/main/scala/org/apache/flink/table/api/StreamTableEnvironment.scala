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

package org.apache.flink.table.api

import _root_.java.util.concurrent.atomic.AtomicInteger
import _root_.java.lang.{Boolean => JBool}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{Expression, WindowEnd}
import org.apache.flink.table.plan.nodes.datastream._
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{DataStreamTable, TableSourceTable}
import org.apache.flink.table.runtime.CRowInputMapRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.types.Row

import _root_.scala.collection.JavaConverters._

/**
  * The base class for stream TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] as a table in the catalog
  * - register a [[Table]] in the catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[StreamTableEnvironment]].
  * @param config The [[TableConfig]] of this [[StreamTableEnvironment]].
  */
abstract class StreamTableEnvironment(
    private[flink] val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // a counter for unique table names
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataStreamTable_[0-9]+$".r

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  override protected def checkValidTableName(name: String): Unit = {
    val m = internalNamePattern.findFirstIn(name)
    m match {
      case Some(_) =>
        throw new TableException(s"Illegal Table name. " +
          s"Please choose a name that does not contain the pattern $internalNamePattern")
      case None =>
    }
  }

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String = "_DataStreamTable_" + nameCntr.getAndIncrement()

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    * The method fails if inputType is not a
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  override protected[flink] def getFieldInfo[A](inputType: TypeInformation[A])
    : (Array[String], Array[Int]) = {
    val fieldInfo = super.getFieldInfo(inputType)
    if (fieldInfo._1.contains("rowtime")) {
      throw new TableException("'rowtime' ia a reserved field name in stream environment.")
    }
    fieldInfo
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]].
    *
    * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  override protected[flink] def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {
    val fieldInfo = super.getFieldInfo(inputType, exprs)
    if (fieldInfo._1.contains("rowtime")) {
      throw new TableException("'rowtime' is a reserved field name in stream environment.")
    }
    fieldInfo
  }

  /**
    * Registers an external [[StreamTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)

    tableSource match {
      case streamTableSource: StreamTableSource[_] =>
        registerTableInternal(name, new TableSourceTable(streamTableSource))
      case _ =>
        throw new TableException("Only StreamTableSource can be registered in " +
            "StreamTableEnvironment")
    }
  }
  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = {

    sink match {

      case retractSink: RetractStreamTableSink[_] =>
        // retraction sink can always be used
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(table, updatesAsRetraction = true, withChangeFlag = true)(outputType)
        // Give the DataStream to the TableSink to emit it.
        retractSink.asInstanceOf[RetractStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])

      case appendSink: AppendStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(table.getRelNode, updatesAsRetraction = false)
        // verify table is an insert-only (append-only) table
        if (!isAppendOnly(optimizedPlan)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(optimizedPlan, table.getRelNode.getRowType, withChangeFlag = false)(outputType)
        // Give the DataStream to the TableSink to emit it.
        appendSink.asInstanceOf[AppendStreamTableSink[T]].emitDataStream(result)

      case upsertSink: UpsertStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(table.getRelNode, updatesAsRetraction = false)
        // extract unique keys
        val tableKeys: Option[Array[String]] = getUniqueKeys(optimizedPlan)
        // check if we have keys and forward to upsert table sink
        tableKeys match {
          case Some(keys) => upsertSink.configureKeyFields(keys)
          case None => throw new TableException(
            "UpsertStreamTableSink requires that Table has unique keys.")
        }
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(optimizedPlan, table.getRelNode.getRowType, withChangeFlag = true)(outputType)
        // Give the DataStream to the TableSink to emit it.
        upsertSink.asInstanceOf[UpsertStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])
      case _ =>
        throw new TableException("StreamTableSink required to emit streaming Table")
    }
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param logicalRowType the logical type with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      logicalRowType: RelDataType,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
    MapFunction[IN, OUT] = {

    if (requestedTypeInfo.getTypeClass == classOf[Row]) {
      // CRow to Row, only needs to be unwrapped
      new MapFunction[CRow, Row] {
        override def map(value: CRow): Row = value.row
      }.asInstanceOf[MapFunction[IN, OUT]]
    } else {
      // Some type that is neither CRow nor Row
      val converterFunction = generateRowConverterFunction[OUT](
        physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
        logicalRowType,
        requestedTypeInfo,
        functionName
      )

      new CRowInputMapRunner[OUT](
        converterFunction.name,
        converterFunction.code,
        converterFunction.returnType)
        .asInstanceOf[MapFunction[IN, OUT]]
    }
  }

  /** Validates that the plan produces only append changes. */
  protected def isAppendOnly(plan: RelNode): Boolean = {
    val appendOnlyValidator = new AppendOnlyValidator
    appendOnlyValidator.go(plan)

    appendOnlyValidator.isAppendOnly
  }

  /** Extracts the unique keys of the table produced by the plan. */
  protected def getUniqueKeys(plan: RelNode): Option[Array[String]] = {
    val keyExtractor = new UniqueKeyExtractor
    keyExtractor.go(plan)
    keyExtractor.keys
  }

  /**
    * Creates a final converter that maps the internal CRow type to Tuple2 with change flag.
    *
    * @param physicalTypeInfo the input of the sink
    * @param logicalRowType the logical type with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getConversionMapperWithChanges[OUT](
      physicalTypeInfo: TypeInformation[CRow],
      logicalRowType: RelDataType,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
    MapFunction[CRow, OUT]

  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataStream.getType)
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Registers a [[DataStream]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T],
    fields: Array[Expression]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataStream.getType, fields)
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Returns the decoration rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getDecoRuleSet: RuleSet = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getDecoRuleSet match {

      case None =>
        getBuiltInDecoRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesDecoRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInDecoRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the environment.
    */
  protected def getBuiltInOptRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_OPT_RULES

  /**
    * Returns the built-in decoration rules that are defined by the environment.
    */
  protected def getBuiltInDecoRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_DECO_RULES

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updatesAsRetraction True if the sink requests updates as retraction messages.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode, updatesAsRetraction: Boolean): RelNode = {

    // 1. decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // 2. normalize the logical plan
    val normRuleSet = getNormRuleSet
    val normalizedPlan = if (normRuleSet.iterator().hasNext) {
      runHepPlanner(HepMatchOrder.BOTTOM_UP, normRuleSet, decorPlan, decorPlan.getTraitSet)
    } else {
      decorPlan
    }

    // 3. optimize the logical Flink plan
    val optRuleSet = getOptRuleSet
    val flinkOutputProps = relNode.getTraitSet.replace(DataStreamConvention.INSTANCE).simplify()
    val optimizedPlan = if (optRuleSet.iterator().hasNext) {
      runVolcanoPlanner(optRuleSet, normalizedPlan, flinkOutputProps)
    } else {
      normalizedPlan
    }

    // 4. decorate the optimized plan
    val decoRuleSet = getDecoRuleSet
    val decoratedPlan = if (decoRuleSet.iterator().hasNext) {
      val planToDecorate = if (updatesAsRetraction) {
        optimizedPlan.copy(
          optimizedPlan.getTraitSet.plus(new UpdateAsRetractionTrait(true)),
          optimizedPlan.getInputs)
      } else {
        optimizedPlan
      }
      runHepPlanner(
        HepMatchOrder.BOTTOM_UP,
        decoRuleSet,
        planToDecorate,
        planToDecorate.getTraitSet)
    } else {
      optimizedPlan
    }
    decoratedPlan
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table, updatesAsRetraction: Boolean, withChangeFlag: Boolean)
      (implicit tpe: TypeInformation[A]): DataStream[A] = {
    val relNode = table.getRelNode
    val dataStreamPlan = optimize(relNode, updatesAsRetraction)
    translate(dataStreamPlan, relNode.getRowType, withChangeFlag)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      withChangeFlag: Boolean)
      (implicit tpe: TypeInformation[A]): DataStream[A] = {

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !isAppendOnly(logicalPlan)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Output needs to handle update and delete changes.")
    }

    // get CRow plan
    val plan: DataStream[CRow] = translateToCRow(logicalPlan)

    // convert CRow to output type
    val conversion = if (withChangeFlag) {
      getConversionMapperWithChanges(plan.getType, logicalType, tpe, "DataStreamSinkConversion")
    } else {
      getConversionMapper(plan.getType, logicalType, tpe, "DataStreamSinkConversion")
    }

    val dopOfPrevious = plan.getParallelism

    conversion match {
      case mapFunction: MapFunction[CRow, A] =>
        plan.map(mapFunction)
          .returns(tpe)
          .name(s"to: ${tpe.getTypeClass.getSimpleName}")
          .setParallelism(dopOfPrevious)
    }
  }

  /**
    * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[CRow]].
    *
    * @param logicalPlan The logical plan to translate.
    * @return The [[DataStream]] of type [[CRow]].
    */
  protected def translateToCRow(
    logicalPlan: RelNode): DataStream[CRow] = {

    logicalPlan match {
      case node: DataStreamRel =>
        node.translateToPlan(this)
      case _ =>
        throw TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = {
    val ast = table.getRelNode
    val optimizedPlan = optimize(ast, updatesAsRetraction = false)
    val dataStream = translateToCRow(optimizedPlan)

    val env = dataStream.getExecutionEnvironment
    val jsonSqlPlan = env.getExecutionPlan

    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jsonSqlPlan, false)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

  private class AppendOnlyValidator extends RelVisitor {

    var isAppendOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: DataStreamRel if s.producesUpdates =>
          isAppendOnly = false
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }

  /** Identifies unique key fields in the output of a RelNode. */
  private class UniqueKeyExtractor extends RelVisitor {

    var keys: Option[Array[String]] = None

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case c: DataStreamCalc =>
          super.visit(node, ordinal, parent)
          // check if input has keys
          if (keys.isDefined) {
            // track keys forward
            val inNames = c.getInput.getRowType.getFieldNames
            val inOutNames = c.getProgram.getNamedProjects.asScala
              .map(p => {
                c.getProgram.expandLocalRef(p.left) match {
                  case i: RexInputRef => (i.getIndex, p.right)
                  case a: RexCall if a.getKind.equals(SqlKind.AS) =>
                    a.getOperands.get(0) match {
                      case ref: RexInputRef =>
                        (ref.getIndex, p.right)
                      case _ =>
                        (-1, p.right)
                    }
                  case _: RexNode => (-1, p.right)
                }
              })
              // filter all non-forwarded fields
              .filter(_._1 >= 0)
              // resolve names of input fields
              .map(io => (inNames.get(io._1), io._2))

            // filter by input keys
            val outKeys = inOutNames.filter(io => keys.get.contains(io._1)).map(_._2)
            if (outKeys.nonEmpty && outKeys.length == keys.get.length) {
              // either all keys are preserved or the table doesn't have unique keys anymore.
              keys = Some(outKeys.toArray)
            } else {
              keys = None
            }
          }
        case _: DataStreamOverAggregate =>
          super.visit(node, ordinal, parent)
          // keys are always forwarded by Over aggregate
        case a: DataStreamGroupAggregate =>
          // get grouping keys
          val groupKeys = a.getRowType.getFieldNames.asScala.take(a.getGroupings.length)
          if (groupKeys.nonEmpty) {
            keys = Some(groupKeys.toArray)
          }
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keys
          val groupKeys =
            w.getRowType.getFieldNames.asScala.take(w.getGroupings.length).toArray
          // get window end time
          val windowEndKey = w.getWindowProperties
            .filter(_.property.child == WindowEnd)
            .map(_.name)
          // combine both to key
          if (groupKeys.nonEmpty || windowEndKey.nonEmpty) {
            keys = Some(groupKeys ++ windowEndKey)
          }
        case _: DataStreamRel =>
          // anything else does not forward keys, so we can stop
          keys = None
      }
    }

  }

}

