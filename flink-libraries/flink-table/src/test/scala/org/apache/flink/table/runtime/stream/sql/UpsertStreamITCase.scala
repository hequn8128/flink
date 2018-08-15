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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.sql.SqlITCase.TimestampWithEqualWatermark
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.types.Row
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import _root_.java.lang.{Boolean => JBool}

import org.junit.Assert._
import org.junit._

class UpsertStreamITCase {

  /** test upsert stream registered table **/
  @Test
  def testRegisterUpsertStream(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    // set parallelism to 1 to ensure data input order, since it is processing time
    env.setParallelism(1)
    StreamITCase.clear

    val data: Seq[JTuple2[JBool, Row]] = List(
      JTuple2.of(true, Row.of("Hello", "Worlds", Int.box(1))),
      JTuple2.of(true, Row.of("Hello", "Hiden", Int.box(5))),
      JTuple2.of(true, Row.of("Hello again", "Worlds", Int.box(2))),
      JTuple2.of(true, Row.of("Hello Flink", "Flink", Int.box(4))),
      JTuple2.of(false, Row.of("Hello again", "Worlds", Int.box(2))))

    val types = Array[TypeInformation[_]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)
    val names = Array("a", "b", "c")
    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(types, names) // tpe is automatically

    val ds: DataStream[JTuple2[JBool, Row]] = env.fromCollectionWithFlag(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val t = tEnv.fromUpsertStream[Row](ds, 'a.key, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)
    val sqlQuery = "SELECT a, b, c FROM MyTableRow"
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello,Hiden,5", "Hello Flink,Flink,4")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  /** test upsert stream registered single row table **/
  @Test
  def testRegisterSingleRowTableFromUpsertStream(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    // set parallelism to 1 to ensure data input order, since it is processing time
    env.setParallelism(1)
    StreamITCase.clear

    val sqlQuery = "SELECT aa, b, c FROM MyTableRow WHERE c < 3"

    val data: Seq[JTuple2[JBool, Row]] = List(
      JTuple2.of(true, Row.of("Hello", "Worlds", Int.box(1))),
      JTuple2.of(true, Row.of("Hello", "Hiden", Int.box(5))),
      JTuple2.of(true, Row.of("Hello again", "Worlds", Int.box(2))))

    val types = Array[TypeInformation[_]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)
    val names = Array("a", "b", "c")
    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(types, names) // tpe is automatically

    val ds: DataStream[JTuple2[JBool, Row]] = env.fromCollectionWithFlag(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val t = tEnv.fromUpsertStream[Row](ds, 'a as 'aa, 'b, 'c, 'rowtime.rowtime, 'proctime.proctime)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello again,Worlds,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

}
