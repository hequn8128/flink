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

package org.apache.flink.table.runtime.stream.sql

import java.io.File

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.java.TablePlannerFactory
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableEnvironment, TablePlanner, Types}
import org.apache.flink.table.factories.TablePlannerUtil
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._


class NewITCase extends StreamingWithStateTestBase {

  /** test unbounded groupBy (without window) **/
  @Test
  def example1(): Unit = {

    StreamITCase.clear

    val config: TableConfig = TableConfig.builder()
//      .asStreamingExecution()
      .asBatchExecution()
      .watermarkInterval(100)
      .build()

    val tEnv = TableEnvironment.create(config)

    // register source
    val csvTable = CommonTestData.getCsvTableSource
    tEnv.registerTableSource("MyTable", csvTable)
    // register sink
    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString
    tEnv.registerTableSink(
      "csvSink",
      new CsvTableSink(path)
        .configure(
          Array[String]("id", "sum"),
          Array[TypeInformation[_]](Types.INT, Types.DOUBLE)))

//    tEnv.scan("MyTable").select('id, 'score).insertInto("csvSink")

    val sqlQuery = "SELECT id, score FROM MyTable"
    tEnv.sqlQuery(sqlQuery).insertInto("csvSink")

    tEnv.execute()

    val expected = Seq(
      "1,12.3", "2,45.6", "3,7.89", "4,0.12", "5,34.5", "6,6.78", "7,90.1", "8,2.34")
      .mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }


  /** test unbounded groupBy (without window) **/
  @Test
  def example2(): Unit = {

    StreamITCase.clear

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // create with StreamTableEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv
      .fromDataStream(ds, 'a, 'b, 'c)
      .select("*")
      .toAppendStream[Row].print

    env.execute()
  }

  /** test unbounded groupBy (without window) **/
  @Test
  def example3(): Unit = {

    StreamITCase.clear

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // createTableEnvironment with TablePlanner
    val tEnv = TablePlanner.createTableEnvironment(env)

    FunctionCatalog.builtInFunctions.map(e => println(e._1))

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv
      .fromDataStream(ds, 'a, 'b, 'c)
      .select("*")
      .toAppendStream[Row].print

    env.execute()
  }

  @Test
  def example4(): Unit = {
    val config: TableConfig = TableConfig.builder()
      //      .asStreamingExecution()
      .asBatchExecution()
      .watermarkInterval(100)
      .build()
    
    TablePlannerUtil.find(classOf[TablePlannerFactory], TablePlannerUtil.generatePlannerDiscriptor(config))
  }
}


