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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.{BatchTop3, BatchTop3WithMerge, EmitRetractTableAggFunc}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableAggregateITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testTop3WithoutGroupBy(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val inputTable = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.registerDataSet("MyTable", inputTable)

    val top3 = new BatchTop3
    val result = tEnv.scan("MyTable")
      .flatAggregate(top3("1".cast(Types.INT), 'a.cast(Types.LONG)))
      .select('_1, '_2, '_3)

    val expected = Seq(
      "1,21,0",
      "1,20,1",
      "1,19,2"
    ).mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTop3WithGroupBy(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val inputTable = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.registerDataSet("MyTable", inputTable)

    val top3 = new BatchTop3
    val result = tEnv.scan("MyTable")
      .groupBy('b)
      .flatAggregate(top3('b.cast(Types.INT), 'a.cast(Types.LONG)))
      .select('_1, '_2, '_3)

    val expected = Seq(
      "1,1,0",
      "2,3,0",
      "2,2,1",
      "3,6,0",
      "3,5,1",
      "3,4,2",
      "4,10,0",
      "4,9,1",
      "4,8,2",
      "5,15,0",
      "5,14,1",
      "5,13,2",
      "6,21,0",
      "6,20,1",
      "6,19,2"
    ).mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTop3WithoutGroupByWithMerge(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val inputTable = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.registerDataSet("MyTable", inputTable)

    val top3 = new BatchTop3WithMerge
    val result = tEnv.scan("MyTable")
      .flatAggregate(top3("1".cast(Types.INT), 'a.cast(Types.LONG)))
      .select('_1, '_2, '_3)

    val expected = Seq(
      "1,21,0",
      "1,20,1",
      "1,19,2"
    ).mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTop3WithGroupByWithMerge(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val inputTable = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.registerDataSet("MyTable", inputTable)

    val top3 = new BatchTop3WithMerge
    val result = tEnv.scan("MyTable")
      .groupBy('b)
      .flatAggregate(top3('b.cast(Types.INT), 'a.cast(Types.LONG)))
      .select('_1, '_2, '_3)

    val expected = Seq(
      "1,1,0",
      "2,3,0",
      "2,2,1",
      "3,6,0",
      "3,5,1",
      "3,4,2",
      "4,10,0",
      "4,9,1",
      "4,8,2",
      "5,15,0",
      "5,14,1",
      "5,13,2",
      "6,21,0",
      "6,20,1",
      "6,19,2"
    ).mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[RuntimeException])
  def testEmitWithRetract(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val inputTable = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.registerDataSet("MyTable", inputTable)

    val top3 = new EmitRetractTableAggFunc
    val result = tEnv.scan("MyTable")
      .groupBy('b)
      .flatAggregate(top3('b.cast(Types.INT), 'a.cast(Types.LONG)))
      .select('_1, '_2, '_3)

    result.toDataSet[Row].collect()
  }
}
