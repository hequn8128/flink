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
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{BatchQueryConfig, Table, TableConfig, TableEnvironment}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

trait BatchTableEnvironment extends TableEnvironment {

  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit

  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: AggregateFunction[T, ACC]): Unit

  def fromDataSet[T](dataSet: DataSet[T]): Table

  def fromDataSet[T](dataSet: DataSet[T], fields: Expression*): Table

  def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit

  def registerDataSet[T](name: String, dataSet: DataSet[T], fields: Expression*): Unit

  def toDataSet[T: TypeInformation](table: Table): DataSet[T]

  def toDataSet[T: TypeInformation](
    table: Table,
    queryConfig: BatchQueryConfig): DataSet[T]
}

object BatchTableEnvironment {

  def create(executionEnvironment: ExecutionEnvironment): BatchTableEnvironment = {
    create(executionEnvironment, TableConfig.DEFAULT)
  }

  def create(executionEnvironment: ExecutionEnvironment, tableConfig: TableConfig)
  : BatchTableEnvironment = {
    val clazz = Class.forName("org.apache.flink.table.api.scala.BatchTableEnvironment")
    val const = clazz.getConstructor(classOf[ExecutionEnvironment], classOf[TableConfig])
    const.newInstance(executionEnvironment, tableConfig).asInstanceOf[BatchTableEnvironment]
  }
}
