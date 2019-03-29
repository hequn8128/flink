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

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, Tumble, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert.fail
import org.junit.Test

class CalcValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .select('rowtime.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .window(Tumble over 2.millis on 'rowtime as 'w)
    .groupBy('w)
    .select('w.end.rowtime, 'int.count as 'int) // no rowtime on non-window reference
  }

  @Test
  def testAddColumns(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Test aggregates
    try {
      tab.addColumns('a.sum)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test replace the existing column, but do not with the alias.
    try {
      tab.addColumns(true, concat('c, "Sunny"))
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

  }

  @Test
  def testRenameColumns(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Test aggregates
    try {
      tab.renameColumns('a.sum)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test without alias case.
    try {
      tab.renameColumns('a)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test function call
    try {
      tab.renameColumns('a + 1  as 'a2)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test for fields that do not exist
    try {
      tab.renameColumns('e as 'e2)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }
  }

  @Test
  def testDropColumns(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Test aggregates
    try {
      tab.dropColumns('a.sum)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test for fields that do not exist
    try {
      tab.dropColumns('e)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    // Test literal.
    try {
      tab.dropColumns("'last'")
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }
  }

}
