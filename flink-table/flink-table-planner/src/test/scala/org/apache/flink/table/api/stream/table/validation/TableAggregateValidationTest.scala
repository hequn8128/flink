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

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase}
import org.junit.Test

class TableAggregateValidationTest extends TableTestBase {

  @Test
  def testInvalidParameterNumber(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long) \nExpected: (long, int), (long, java.sql.Timestamp)")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func take 2 parameters
      .flatAggregate(func('a))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidParameterType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long, java.lang.String) \n" +
      "Expected: (long, int), (long, java.sql.Timestamp)")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func take 2 parameters of type Long and Timestamp
      .flatAggregate(func('a, 'c))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidWithWindowProperties(): Unit = {
//    expectedException.expect(classOf[ValidationException])
//    expectedException.expectMessage("Window properties can only be used on windowed tables.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      .flatAggregate(func('a, 'b) as ('a, 'b, 'c))
      .select('a.start, 'b)
  }

  @Test
  def testInvalidParameterWithAgg(): Unit = {
//    expectedException.expect(classOf[ValidationException])
//    expectedException.expectMessage("It's not allowed to use an aggregate function:" +
//      " [class org.apache.flink.table.expressions.Sum] as input of table aggregate " +
//      "function: [EmptyTableAggFunc]")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. func take agg function as input
      .flatAggregate(func('a.sum, 'c))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidAliasWithWrongNumber(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("List of column aliases must have same degree as " +
      "table; the returned table of function 'org.apache.flink.table.utils.EmptyTableAggFunc' " +
      "has 3 columns, whereas alias list has 2 columns")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with wrong number of fields
      .flatAggregate(func('a, 'b) as ('a, 'b))
      .select('*)
  }

  @Test
  def testAliasWithNameConflict(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Field names must be unique.\nList of duplicate " +
      "fields: [b]\nList of all fields: [b, a, b, c]")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with wrong number of fields
      .flatAggregate(func('a, 'b) as ('a, 'b, 'c))
      .select('*)
  }

  @Test
  def testInvalidDistinct(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A flatAggregate only accepts an expression which " +
      "defines a table aggregate function that might be followed by some alias.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      .flatAggregate(func('a, 'b).distinct as ('a, 'b, 'c))
      .select('a, 'b)
  }
}
