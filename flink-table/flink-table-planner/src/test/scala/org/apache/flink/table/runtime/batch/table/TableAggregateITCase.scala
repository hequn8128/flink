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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.{Top3, Top3WithMerge}
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
  def testGroupByFlatAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val top3WithMerge = new Top3

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3WithMerge('a) as ('x, 'y))
      .select('b, 'x, 'y)

    val results = t.toDataSet[Row].collect()
    val expected = "1,1,1\n2,2,2\n2,3,3\n3,4,4\n3,5,5\n3,6,6\n4,10,10\n4,8,8\n4,9,9\n" +
      "5,13,13\n5,14,14\n5,15,15\n6,19,19\n6,20,20\n6,21,21"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNonkeyedFlatAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val top3WithMerge = new Top3

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .flatAggregate(top3WithMerge('a))
      .select('f0, 'f1)
      .as('v1, 'v2)

    val results = t.toDataSet[Row].collect()
    val expected = "21,21\n20,20\n19,19"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupByFlatAggregateWithPreFinal(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val top3WithMerge = new Top3WithMerge

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3WithMerge('a) as ('x, 'y))
      .select('b, 'x, 'y)

    val results = t.toDataSet[Row].collect()
    val expected = "1,1,1\n2,2,2\n2,3,3\n3,4,4\n3,5,5\n3,6,6\n4,10,10\n4,8,8\n4,9,9\n" +
      "5,13,13\n5,14,14\n5,15,15\n6,19,19\n6,20,20\n6,21,21"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNonkeyedFlatAggregateWithPreFinal(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val top3WithMerge = new Top3WithMerge

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .flatAggregate(top3WithMerge('a))
      .select('f0, 'f1)
      .as('v1, 'v2)

    val results = t.toDataSet[Row].collect()
    val expected = "21,21\n20,20\n19,19"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
