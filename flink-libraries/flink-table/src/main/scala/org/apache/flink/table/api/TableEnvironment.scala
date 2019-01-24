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

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.Lex
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.plan.{Convention, RelOptPlanner, RelOptUtil, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{RowTypeInfo, _}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogSchema}
import org.apache.flink.table.codegen.{ExpressionReducer, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.cost.DataSetCostFactory
import org.apache.flink.table.plan.logical.{CatalogNode, LogicalRelNode}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{RelTable, RowSchema, TableSourceSinkTable}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.types.Row

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.api.scala.{ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.api.java.{StreamTablePlanner => JavaStreamTablePlanner}
import org.apache.flink.table.api.scala.{BatchTablePlanner => ScalaBatchTablePlanner, StreamTablePlanner => ScalaStreamTablePlanner}

import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnv}

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(private[flink] val tablePlanner: TablePlanner) {

  /**
    * Creates a table from a table source.
    *
    * @param source table source used as table
    */
  def fromTableSource(source: TableSource[_]): Table = {
    tablePlanner.fromTableSource(source)
  }

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    tablePlanner.registerExternalCatalog(name, externalCatalog)
  }

  /**
    * Gets a registered [[ExternalCatalog]] by name.
    *
    * @param name The name to look up the [[ExternalCatalog]]
    * @return The [[ExternalCatalog]]
    */
  def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    tablePlanner.getRegisteredExternalCatalog(name)
  }

  /**
    * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerFunction(name: String, function: ScalarFunction): Unit = {
    tablePlanner.registerFunction(name, function)
  }

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {
    tablePlanner.registerTable(name, table)
  }


  /**
    * Registers an external [[TableSource]] in this [[TablePlanner]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    tablePlanner.registerTableSource(name, tableSource)
  }

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TablePlanner]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    tablePlanner.registerTableSink(name, configuredSink)
  }

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the TableEnvironment. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
    *
    * Examples:
    *
    * - Scanning a directly registered table
    * {{{
    *   val tab: Table = tableEnv.scan("tableName")
    * }}}
    *
    * - Scanning a table from a registered catalog
    * {{{
    *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
    * }}}
    *
    * @param tablePath The path of the table to scan.
    * @throws TableException if no table is found using the given table path.
    * @return The resulting [[Table]].
    */
  @throws[TableException]
  @varargs
  def scan(tablePath: String*): Table = {
    tablePlanner.scan(tablePath: _*)
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sqlQuery(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table
    */
  def sqlQuery(query: String): Table = {
    tablePlanner.sqlQuery(query)
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    */
  def sqlUpdate(stmt: String): Unit = {
    tablePlanner.sqlUpdate(stmt)
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    * @param config The [[QueryConfig]] to use.
    */
  def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    tablePlanner.sqlUpdate(stmt, config)
  }

  def execute()
}

object TableEnvironment {

  def create(tableConfig: TableConfig): TableEnvironment = {

    // 1. Discover StreamPlanner
    tableConfig.getExecutionMode match {
      case ExecutionMode.StreamingMode =>
        val env = ScalaStreamExecEnv.getExecutionEnvironment
        env.getConfig.setAutoWatermarkInterval(tableConfig.getWatermarkInterval)
        val streamTablePlanner = new ScalaStreamTablePlanner(env, tableConfig)
        new ScalaStreamTableEnv(streamTablePlanner)

      case ExecutionMode.BatchMode =>
        val env = ScalaBatchExecEnv.getExecutionEnvironment
        val batchTablePlanner = new ScalaBatchTablePlanner(env, tableConfig)
        new ScalaBatchTableEnv(batchTablePlanner)
    }

    // 2. StreamPlanner configures itself with the given TableConfig (e.g. watermark interval)

    // 3. Table environment calls are forwarded to the planner
  }
}



