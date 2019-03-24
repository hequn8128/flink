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
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ExternalCatalog
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.sinks.TableSink

/**
  * The [[TableEnvImpl]] for a Scala [[StreamExecutionEnvironment]].
  */
class BatchTableEnvImpl(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.BatchTableEnvImpl(
    execEnv.getWrappedStreamExecutionEnvironment,
    config)
    with org.apache.flink.table.api.scala.BatchTableEnvironment {

  override def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = ???

  override def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: AggregateFunction[T, ACC]): Unit = ???

  override def fromDataSet[T](dataSet: DataSet[T]): Table = ???

  override def fromDataSet[T](dataSet: DataSet[T], fields: Expression*): Table = ???

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = ???

  override def registerDataSet[T](
    name: String,
    dataSet: DataSet[T],
    fields: Expression*): Unit = ???

  override def toDataSet[T: TypeInformation](table: Table): DataSet[T] = ???

  override def toDataSet[T: TypeInformation](
    table: Table,
    queryConfig:
    BatchQueryConfig): DataSet[T] = ???
}

