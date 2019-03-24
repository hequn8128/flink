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
package org.apache.flink.table.api.java

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{BatchQueryConfig, Table, TableConfig, TableEnvImpl}
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

/**
  * The [[TableEnvImpl]] for a Java [[StreamExecutionEnvironment]].
  */
class BatchTableEnvImpl(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.BatchTableEnvImpl(execEnv, config)
    with org.apache.flink.table.api.java.BatchTableEnvironment {

  override def registerFunction[T](name: String, tableFunction: TableFunction[T]): Unit = ???

  override def registerFunction[T, ACC](
    name: String,
    aggregateFunction: AggregateFunction[T, ACC]): Unit = ???

  override def fromDataSet[T](dataSet: DataSet[T]): Table = ???

  override def fromDataSet[T](dataSet: DataSet[T], fields: String): Table = ???

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = ???

  override def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit = ???

  override def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = ???

  override def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = ???

  override def toDataSet[T](
    table: Table,
    clazz: Class[T],
    queryConfig: BatchQueryConfig): DataSet[T] = ???

  override def toDataSet[T](
    table: Table,
    typeInfo: TypeInformation[T],
    queryConfig: BatchQueryConfig): DataSet[T] = ???
}

