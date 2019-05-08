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
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Test for testing aggregate plans.
  */
class TableAggregateTest extends TableTestBase {

  val util = batchTestUtil()
  val table = util.addTable[(Long, Int, Long)]("MyTable", 'a, 'b, 'c)
  val emptyFunc = new EmptyTableAggFunc

  @Test
  def testTableAggregateWithGroupBy(): Unit = {

    val resultTable = table
      .groupBy('b % 5 as 'bb)
      .flatAggregate(emptyFunc('a, 'b) as ('x, 'y))
      .select('bb, 'x + 1, 'y)

    val expected =
      unaryNode("DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "b", "MOD(b, 5) AS bb")),
          term("groupBy", "bb"),
          term("select", "bb", "EmptyTableAggFunc(a, b) AS f0")
        ),
        term("select", "bb", "+(f0, 1) AS _c1", "f1 AS y")
      )

    util.verifyTable(resultTable,expected)
  }

  @Test
  def testTableAggregateWithoutGroupBy(): Unit = {

    val resultTable = table
      .flatAggregate(emptyFunc('a, 'b))
      .select(Func0('f0) as 'a, 'f1 as 'b)

    val expected =
      unaryNode("DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "b")),
          term(
            "select", "EmptyTableAggFunc(a, b) AS f0")
        ),
        term("select", "Func0$(f0) AS a", "f1 AS b")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithSelectStar(): Unit = {

    val resultTable = table
      .flatAggregate(emptyFunc('b))
      .select("*")

    val expected = unaryNode(
      "DataSetTableAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "b")
      ),
      term("select", "EmptyTableAggFunc(b) AS f0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithAlias(): Unit = {
    val resultTable = table
      .flatAggregate(emptyFunc('b) as ('a, 'b))
      .select('a, 'b)

    val expected =
      unaryNode("DataSetCalc",
        unaryNode(
          "DataSetTableAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "b")),
          term(
            "select", "EmptyTableAggFunc(b) AS f0")
        ),
        term("select", "f0 AS a", "f1 AS b")
      )

    util.verifyTable(resultTable, expected)
  }
}
