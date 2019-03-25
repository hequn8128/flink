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
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor, TableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

trait StreamTableEnvironment extends TableEnvironment {

  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit

  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: AggregateFunction[T, ACC]): Unit

  def fromDataStream[T](dataStream: DataStream[T]): Table

  def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table

  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit

  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: Expression*): Unit

  def toAppendStream[T: TypeInformation](table: Table): DataStream[T]

  def toAppendStream[T: TypeInformation](
    table: Table,
    queryConfig: StreamQueryConfig): DataStream[T]

  def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)]

  def toRetractStream[T: TypeInformation](
    table: Table,
    queryConfig: StreamQueryConfig): DataStream[(Boolean, T)]

  override def connect(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor;
}

object StreamTableEnvironment {

  def create(executionEnvironment: StreamExecutionEnvironment): StreamTableEnvironment = {
    create(executionEnvironment, new TableConfig)
  }

  def create(executionEnvironment: StreamExecutionEnvironment, tableConfig: TableConfig)
  : StreamTableEnvironment = {
    try {
      val clazz = Class.forName("org.apache.flink.table.api.scala.StreamTableEnvImpl")
      val const = clazz.getConstructor(classOf[StreamExecutionEnvironment], classOf[TableConfig])
      const.newInstance(executionEnvironment, tableConfig).asInstanceOf[StreamTableEnvironment]
    } catch {
      case t: Throwable => throw new TableException("Create StreamTableEnvImpl failed.", t)
    }
  }
}
