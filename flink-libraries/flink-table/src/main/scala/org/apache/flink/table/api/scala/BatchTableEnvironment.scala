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
package org.apache.flink.table.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{BatchQueryConfig, Table, TablePlanner}
import org.apache.flink.table.expressions.Expression

/**
  * The [[TablePlanner]] for a Scala [[StreamExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[TablePlanner]]'s catalog
  * - register a [[Table]] in the [[TablePlanner]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class BatchTableEnvironment(tablePlanner: BatchTablePlanner)
  extends org.apache.flink.table.api.BatchTableEnvironment(tablePlanner) {

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T]): Table = {
    tablePlanner.fromDataSet(dataSet)
  }

  /**
    * Converts the given [[DataSet]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataSet(set, 'a, 'b)
    * }}}
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T], fields: Expression*): Table = {
    tablePlanner.fromDataSet(dataSet, fields: _*)
  }

  /**
    * Registers the given [[DataSet]] as table in the
    * [[TablePlanner]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = {
    tablePlanner.registerDataSet(name, dataSet)
  }

  /**
    * Registers the given [[DataSet]] as table with specified field names in the
    * [[TablePlanner]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   tableEnv.registerDataSet("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T], fields: Expression*): Unit = {
    tablePlanner.registerDataSet(name, dataSet)
  }

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation](table: Table): DataSet[T] = {
    tablePlanner.toDataSet(table)
  }

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation](table: Table, queryConfig: BatchQueryConfig): DataSet[T] = {
    tablePlanner.toDataSet(table, queryConfig)
  }

  override def execute(): Unit = {
    tablePlanner.execEnv.execute()
  }
}



