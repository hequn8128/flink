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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.table.{TestAppendSink, TestUpsertSink}
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Test

class TableSinkValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    .writeToSink(new TestAppendSink)

    // must fail because table is not append-only
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO) // tpe is automatically

    val t = tEnv.fromUpsertStream(
      StreamTestData.getSmall3TupleDataStreamWithFlag(env), 'id, 'num, 'text)

    t.groupBy('text)
      .select('text, 'id.count, 'num.sum)
      .writeToSink(new TestAppendSink)

    // must fail because table is not append-only
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
    .groupBy('len, 'cTrue)
    .select('len, 'id.count, 'num.sum)
    .writeToSink(new TestUpsertSink(Array("len", "cTrue"), false))

    // must fail because table is updating table without full key
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnLeftJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h)
      .select('c, 'g)
      .writeToSink(new TestAppendSink)

    // must fail because table is not append-only
    env.execute()
  }
}
