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

package org.apache.flink.table.api.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.Func0
import org.apache.flink.table.utils.{TableTestBase, TopN}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Test for testing table aggregate plans.
  */
class TableAggregateTest extends TableTestBase {

  @Test
  def testGroupAggregateWithFilter(): Unit = {

    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .groupBy('b % 5)
      .flatAggregate(top3('a + 1, 'b))
      .select('_1 + 1 as 'a, '_2 as 'b, '_3 as 'c)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "b", "MOD(b, 5) AS $f2", "+(a, 1) AS $f3")
          ),
          term("groupBy", "$f2"),
          term("flatAggregate", "TopN($f3, b) AS (_1, _2, _3)")
        ),
        term("select", "+(_1, 1) AS a", "_2 AS b", "_3 AS c")
      )

    util.verifyTable(resultTable,expected)
  }


  @Test
  def testTableAggregateWithoutGroupBy(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .flatAggregate(top3('a, 'b))
      .select(Func0('_1) as 'a, '_2 as 'b, '_3 as 'c)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          batchTableNode(0),
          term("flatAggregate", "TopN(a, b) AS (_1, _2, _3)")
        ),
        term("select", "Func0$(_1) AS a", "_2 AS b", "_3 AS c")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithGroupByForString(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val top3 = new TopN(3)
    util.tableEnv.registerFunction("top3", top3)
    val resultTable = table
      .groupBy("b % 5")
      .flatAggregate("Top3(a + 1, b)")
      .select("_1 + 1 as a, _2 as b, _3 as c")

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "b", "MOD(b, 5) AS $f2", "+(a, 1) AS $f3")
          ),
          term("groupBy", "$f2"),
          term("flatAggregate", "TopN($f3, b) AS (_1, _2, _3)")
        ),
        term("select", "+(_1, 1) AS a", "_2 AS b", "_3 AS c")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithoutGroupByForString(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val top3 = new TopN(3)
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)
    val resultTable = table
      .flatAggregate("top3(a, b)")
      .select("Func0(_1) as a, _2 as b, _3 as c")

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          batchTableNode(0),
          term("flatAggregate", "TopN(a, b) AS (_1, _2, _3)")
        ),
        term("select", "Func0$(_1) AS a", "_2 AS b", "_3 AS c")
      )
    util.verifyTable(resultTable, expected)
  }
}
