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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.WindowReference
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.{TableTestBase, TopN}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class TableAggregateWindowTest extends TableTestBase {

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .flatAggregate(top3('int, 'long))
      .select('w1.proctime as 'proctime, 'string, '_1, '_2 + 1 as '_2, '_3)
      .window(Slide over 20.milli every 10.milli on 'proctime as 'w2)
      .groupBy('w2)
      .flatAggregate(top3('_1, '_2))
      .select('w2.start, '_1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowAggregate",
            streamTableNode(0),
            term("groupBy", "string"),
            term(
              "window",
              TumblingGroupWindow(
                WindowReference("w1"),
                'proctime,
                50.milli)),
            term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
            term("select", "string", "_1", "_2", "_3", "proctime('w1) AS TMP_0")
          ),
          term("select", "_1", "+(_2, 1) AS _2", "TMP_0 AS proctime")
        ),
        term(
          "window",
          SlidingGroupWindow(
            WindowReference("w2"),
            'proctime,
            20.milli,
            10.milli)),
        term("flatAggregate", "TopN(_1, _2) AS (_1, _2, _3)"),
        term("select", "_1", "_2", "_3", "start('w2) AS TMP_1")
      ),
      term("select", "TMP_1", "_1")
    )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .flatAggregate(top3('int, 'long))
      .select('w1.proctime as 'proctime, 'string, '_1, '_2 + 1, '_3)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          streamTableNode(0),
          term("groupBy", "string"),
          term(
            "window",
            TumblingGroupWindow(
              WindowReference("w1"),
              'proctime,
              50.milli)),
          term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
          term("select", "string", "_1", "_2", "_3", "proctime('w1) AS TMP_0")
        ),
        term("select",
          "PROCTIME(TMP_0) AS proctime", "string", "_1", "+(_2, 1) AS _c3", "_3")
      )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(WindowReference("w"), 'proctime, 2.rows)),
      term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
      term("select", "string", "_1", "_2", "_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "CAST(long) AS long", "string")
      ),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'long,
          5.milli)),
      term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
      term("select", "string", "_1", "_2", "_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('w.proctime as 'proctime, 'string, '_1, '_2 + 1, '_3)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          streamTableNode(0),
          term("groupBy", "string"),
          term(
            "window",
            SlidingGroupWindow(
              WindowReference("w"),
              'proctime,
              50.milli,
              50.milli)),
          term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
          term("select", "string", "_1", "_2", "_3", "proctime('w) AS TMP_0")
        ),
        term("select",
          "PROCTIME(TMP_0) AS proctime", "string", "_1", "+(_2, 1) AS _c3", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        SlidingGroupWindow(WindowReference("w"), 'proctime, 2.rows, 1.rows)),
      term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
      term("select", "string", "_1", "_2", "_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 8.millis, 10.millis)),
      term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
      term("select", "string", "_1", "_2", "_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('string, '_1, '_2, '_3)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "CAST(long) AS long", "string")
      ),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'long, 7.milli)),
      term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
      term("select", "string", "_1", "_2", "_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "proctime")
        ),
        term(
          "window",
          TumblingGroupWindow(
            WindowReference("w"),
            'proctime,
            50.milli)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "proctime")
        ),
        term(
          "window",
          TumblingGroupWindow(
            WindowReference("w"),
            'proctime,
            2.rows)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "rowtime")
        ),
        term("window", TumblingGroupWindow(WindowReference("w"), 'rowtime, 5.milli)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "proctime")
        ),
        term(
          "window",
          SlidingGroupWindow(
            WindowReference("w"),
            'proctime,
            50.milli,
            50.milli)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "proctime")
        ),
        term(
          "window",
          SlidingGroupWindow(
            WindowReference("w"),
            'proctime,
            2.rows,
            1.rows)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "long", "rowtime")
        ),
        term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 8.milli, 10.milli)),

        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "CAST(long) AS long")
        ),
        term("window", SlidingGroupWindow(WindowReference("w"), 'long, 8.milli, 10.milli)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "int", "CAST(long) AS long")
        ),
        term(
          "window",
          SessionGroupWindow(
            WindowReference("w"),
            'long,
            7.milli)),
        term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
        term("select",  "_1", "_2", "_3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          streamTableNode(0),
          term("groupBy", "string"),
          term("window", TumblingGroupWindow(WindowReference("w"), 'rowtime, 5.milli)),
          term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
          term("select",
            "string", "_1", "_2", "_3", "start('w) AS TMP_0", "end('w) AS TMP_1")
        ),
        term("select", "_1", "_2", "_3", "TMP_0", "TMP_1")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('_1, '_2, '_3, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          streamTableNode(0),
          term("groupBy", "string"),
          term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 10.milli, 5.milli)),
          term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
          term("select",
            "string", "_1", "_2", "_3", "start('w) AS TMP_0", "end('w) AS TMP_1")
        ),
        term("select", "_1", "_2", "_3", "TMP_0", "TMP_1")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val top3 = new TopN(3)
    val windowedTable = table
      .window(Session withGap 3.milli on 'long as 'w)
      .groupBy('w, 'string)
      .flatAggregate(top3('int, 'long))
      .select('w.end as 'we1, '_1, '_2, '_3, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "int", "CAST(long) AS long", "string")
          ),
          term("groupBy", "string"),
          term("window", SessionGroupWindow(WindowReference("w"), 'long, 3.milli)),
          term("flatAggregate", "TopN(int, long) AS (_1, _2, _3)"),
          term("select",
            "string", "_1", "_2", "_3", "end('w) AS TMP_0", "start('w) AS TMP_1")
        ),
        term("select", "TMP_0 AS we1", "_1", "_2", "_3", "TMP_1", "TMP_0")
      )

    util.verifyTable(windowedTable, expected)
  }
}
