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

import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.sinks.TableSink

trait Table {

  def getSchema: TableSchema

  def printSchema(): Unit

  def select(fields: Expression*): Table

  def select(fields: String): Table

  def createTemporalTableFunction(
    timeAttribute: String,
    primaryKey: String): TemporalTableFunction

  def createTemporalTableFunction(
    timeAttribute: Expression,
    primaryKey: Expression): TemporalTableFunction

  def as(fields: Expression*): Table

  def as(fields: String): Table

  def filter(predicate: Expression): Table

  def filter(predicate: String): Table

  def where(predicate: Expression): Table

  def where(predicate: String): Table

  def groupBy(fields: Expression*): GroupedTable

  def groupBy(fields: String): GroupedTable

  def distinct(): Table

  def join(right: Table): Table

  def join(right: Table, joinPredicate: String): Table

  def join(right: Table, joinPredicate: Expression): Table

  def leftOuterJoin(right: Table): Table

  def leftOuterJoin(right: Table, joinPredicate: String): Table

  def leftOuterJoin(right: Table, joinPredicate: Expression): Table

  def rightOuterJoin(right: Table, joinPredicate: String): Table

  def rightOuterJoin(right: Table, joinPredicate: Expression): Table

  def fullOuterJoin(right: Table, joinPredicate: String): Table

  def fullOuterJoin(right: Table, joinPredicate: Expression): Table

  def joinLateral(tableFunctionCall: String): Table

  def joinLateral(tableFunctionCall: String, joinPredicate: String): Table

  def joinLateral(tableFunctionCall: Expression): Table

  def joinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table

  def leftOuterJoinLateral(tableFunctionCall: String): Table

  def leftOuterJoinLateral(tableFunctionCall: String, joinPredicate: String): Table

  def leftOuterJoinLateral(tableFunctionCall: Expression): Table

  def leftOuterJoinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table

  def minus(right: Table): Table

  def minusAll(right: Table): Table

  def union(right: Table): Table

  def unionAll(right: Table): Table

  def intersect(right: Table): Table

  def intersectAll(right: Table): Table

  def orderBy(fields: Expression*): Table

  def orderBy(fields: String): Table

  def offset(offset: Int): Table

  def fetch(fetch: Int): Table

  def insertInto(tableName: String): Unit

  def insertInto(tableName: String, conf: QueryConfig): Unit

  def window(window: Window): WindowedTable

  def window(overWindows: OverWindow*): OverWindowedTable

  @Deprecated
  def writeToSink[T](sink: TableSink[T]): Unit

  @Deprecated
  def writeToSink[T](sink: TableSink[T], conf: QueryConfig): Unit

  def getTableEnvironment(): TableEnvironment
}
