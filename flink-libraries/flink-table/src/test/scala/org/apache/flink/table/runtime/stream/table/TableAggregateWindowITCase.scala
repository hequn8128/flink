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

package org.apache.flink.table.runtime.stream.table

import java.math.BigDecimal

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment, Types}
import org.apache.flink.table.runtime.stream.table.GroupWindowITCase._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.utils.{BatchTop3, BatchTop3WithMerge}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
  * IT Tests for Table Aggregations.
  */
class TableAggregateWindowITCase extends AbstractTestBase {
  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  val data2 = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val top3 = new BatchTop3
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val results = windowedTable.toAppendStream[Row](queryConfig)
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello world,3,8,0", "Hello world,3,16,0", "Hello world,3,8,1",
      "Hello,2,2,0", "Hello,2,4,0", "Hello,2,2,1", "Hi,1,1,0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestdata = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(sessionWindowTestdata)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new BatchTop3WithMerge
    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('string.charLength(), 'long))
      .select('string, '_1, '_2, '_3)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello World,11,9,0", "Hello,5,8,0", "Hello,5,4,1", "Hello,5,2,2", "Hello,5,16,0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)
    val top3 = new BatchTop3

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3("1".cast(Types.INT), 'long))
      .select('_1, '_2 + 1, '_3)

    val results = windowedTable.toAppendStream[Row](queryConfig)
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("1,2,1", "1,3,0", "1,5,1", "1,9,0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = StreamTestData.get3TupleDataStream(env)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Int, Long, String)](0L))
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'rowtime.rowtime)

    val top3 = new BatchTop3

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'long)
      .flatAggregate(top3('long.cast(Types.INT), 'int.cast(Types.LONG)))
      .select('w.start, 'w.end, 'long, '_1, '_2 + 1, '_3)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1,1,2,0",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2,2,3,1",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2,2,4,0",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,3,3,5,0",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,3,3,6,1",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,3,3,7,0",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,4,10,0",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,4,8,2",
      "1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,4,4,9,1",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,4,4,11,0",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,5,13,2",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,5,14,1",
      "1970-01-01 00:00:00.01,1970-01-01 00:00:00.015,5,5,15,0",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,5,5,16,0",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,6,18,2",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,6,19,1",
      "1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,6,6,20,0",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.025,6,6,21,1",
      "1970-01-01 00:00:00.02,1970-01-01 00:00:00.025,6,6,22,0")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("1,1,0", "2,2,0", "2,4,0", "2,2,1", "3,8,0", "3,16,0", "3,8,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'long as 'w)
      .groupBy('w)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('_1, '_2, '_3, 'w.start, 'w.end, 'w.rowtime)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1,1,1,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003,1970-01-01 00:00:00.002",
      "1,2,0,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003,1970-01-01 00:00:00.002",
      "1,2,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "1,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "1,5,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "1,2,1,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "1,2,2,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "1,5,0,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "1,3,1,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "1,3,2,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "1,5,0,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "1,3,0,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011,1970-01-01 00:00:00.01",
      "1,3,1,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011,1970-01-01 00:00:00.01",
      "1,3,0,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013,1970-01-01 00:00:00.012",
      "1,4,0,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017,1970-01-01 00:00:00.016",
      "1,4,0,1970-01-01 00:00:00.014,1970-01-01 00:00:00.019,1970-01-01 00:00:00.018",
      "1,4,0,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021,1970-01-01 00:00:00.02",
      "1,4,0,1970-01-01 00:00:00.028,1970-01-01 00:00:00.033,1970-01-01 00:00:00.032",
      "1,4,0,1970-01-01 00:00:00.03,1970-01-01 00:00:00.035,1970-01-01 00:00:00.034",
      "1,4,0,1970-01-01 00:00:00.032,1970-01-01 00:00:00.037,1970-01-01 00:00:00.036")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('string, '_1, '_2, '_3, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,2,0,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hallo,1,2,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,3,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,3,0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello world,1,4,0,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02",
      "Hello world,1,4,0,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025",
      "Hello,1,2,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,1,5,0,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,1,2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,1,3,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,1,5,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello,1,3,0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hi,1,1,0,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hi,1,1,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "null,1,4,0,1970-01-01 00:00:00.025,1970-01-01 00:00:00.035",
      "null,1,4,0,1970-01-01 00:00:00.03,1970-01-01 00:00:00.04")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val top3 = new BatchTop3
    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 4.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('string, '_1, '_2, '_3, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,2,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,1,2,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,1,5,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,1,3,1,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello,1,5,0,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,1,3,0,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,1,3,0,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "Hello world,1,4,0,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "Hello world,1,4,0,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "Hi,1,1,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "null,1,4,0,1970-01-01 00:00:00.028,1970-01-01 00:00:00.033",
      "null,1,4,0,1970-01-01 00:00:00.032,1970-01-01 00:00:00.037")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 5.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('string, '_1, '_2, '_3, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,2,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,1,2,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,1,5,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "null,1,4,0,1970-01-01 00:00:00.03,1970-01-01 00:00:00.035")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('string, '_1, '_2, '_3, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hallo,1,2,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "null,1,4,0,1970-01-01 00:00:00.03,1970-01-01 00:00:00.033")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeGroupWindowWithoutExplicitTimeField(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
      .map(t => (t._2, t._6))
    val table = stream.toTable(tEnv, 'int, 'string, 'rowtime.rowtime)

    val top3 = new BatchTop3
    val windowedTable = table
      .window(Slide over 3.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3("1".cast(Types.INT), 'int.cast(Types.LONG)))
      .select('string, '_1, '_2, '_3, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = Seq(
      "Hallo,1,2,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "Hi,1,1,0,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
      "null,1,4,0,1970-01-01 00:00:00.03,1970-01-01 00:00:00.033")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

