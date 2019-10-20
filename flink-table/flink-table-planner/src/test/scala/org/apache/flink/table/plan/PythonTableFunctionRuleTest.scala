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

package org.apache.flink.table.plan

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.python.PythonEnv.ExecType
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.types.Row
import org.junit.Test

class PythonTableFunctionRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonTableFunction("pyTableFunc1"))

    val resultTable = table
      .joinLateral("pyFunc1(a)")

    val expected = unaryNode(
      "DataStreamPythonCorrelate",
      streamTableNode(table),
      term("select", "pyFunc1(a, b) AS f0")
    )

    util.verifyTable(resultTable, expected)
  }

}

class PythonTableFunction(name: String) extends TableFunction[Row] with PythonFunction {
  def eval(i: Int): Unit = {}
  override def getResultType(): TypeInformation[Row] = Types.ROW(Types.INT(), Types.INT())
  override def getSerializedPythonFunction: Array[Byte] = Array[Byte]()
  override def getPythonEnv: PythonEnv = new PythonEnv(ExecType.PROCESS)
  override def toString: String = name
}

