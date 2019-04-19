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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.Func0
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase, Top3WithMapView}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.types.Row
import org.junit.Test

class TableAggregateTest extends TableTestBase {

  @Test
  def testTableAggregateWithGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    val resultTable = table
      .groupBy('b % 5)
      .flatAggregate(top3('a))
      .select('f0 + 1, 'f1 as 'b)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "MOD(b, 5) AS $f3")
          ),
          term("groupBy", "$f3"),
          term("flatAggregate", "Top3WithMapView(a) AS (f0, f1)")
        ),
        term("select", "+(f0, 1) AS _c0", "f1 AS b")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithoutGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    val resultTable = table
      .flatAggregate(top3('a))
      .select(Func0('f0) as 'a, 'f1 as 'b)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a")
          ),
          term("flatAggregate", "Top3WithMapView(a) AS (f0, f1)")
        ),
        term("select", "Func0$(f0) AS a", "f1 AS b")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithTimeIndicator(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Long)]('a.rowtime, 'b, 'c, 'd.proctime)

    val emptyFunc = new EmptyTableAggFunc
    val resultTable = table
      .flatAggregate(emptyFunc('a, 'd))
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "CAST(a) AS a", "PROCTIME(d) AS d")
        ),
        term("flatAggregate", "EmptyTableAggFunc(a, d) AS (_1, _2, _3)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithSelectStar(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    val resultTable = table
      .flatAggregate(top3('a))
      .select("*")

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a")
        ),
        term("flatAggregate", "Top3WithMapView(a) AS (f0, f1)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    val resultTable = table
      .flatAggregate(top3('a) as ('a, 'b))
      .select('a, 'b)

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a")
        ),
        term("flatAggregate", "Top3WithMapView(a) AS (a, b)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testJavaRegisterFunction(): Unit = {
    val util = streamTestUtil()
    val typeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)
    val table = util.addJavaTable[Row](typeInfo, "sourceTable", "a, b, c")

    val top3 = new Top3WithMapView
    util.javaTableEnv.registerFunction("top3", top3)

    val resultTable = table
      .groupBy("a")
      .flatAggregate("top3(a) as (b, c)")
      .select("*")

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a")
        ),
        term("groupBy", "a"),
        term("flatAggregate", "Top3WithMapView(a) AS (b, c)")
      )
    util.verifyJavaTable(resultTable, expected)
  }
}

