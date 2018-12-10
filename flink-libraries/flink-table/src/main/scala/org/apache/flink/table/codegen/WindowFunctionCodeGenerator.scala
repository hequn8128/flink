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
package org.apache.flink.table.codegen

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, RichAllWindowFunction, RichWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.aggregate.AggregateUtil.computeWindowPropertyPos
import org.apache.flink.table.runtime.aggregate.{DataStreamTimeWindowPropertyCollector => TimeWindowPropertyCollector}
import org.apache.flink.util.Collector
import java.util.{LinkedList => JList}

/**
  * A code generator for generating Flink [[AllWindowFunction]]s or [[WindowFunction]]s.
  *
  * @param config configuration that determines runtime behavior
  */
class WindowFunctionCodeGenerator(
    config: TableConfig)
  extends CodeGenerator(config, false, new RowTypeInfo(), None, None) {

  private val cRowClassName = classOf[CRow].getCanonicalName
  private val rowClassName = classOf[Row].getCanonicalName
  private val windowClassName = classOf[Window].getCanonicalName
  private val timeWindowClassName = classOf[TimeWindow].getCanonicalName
  private val timeCollectorClassName = classOf[TimeWindowPropertyCollector].getCanonicalName
  private val javaIterableClassName = classOf[java.lang.Iterable[_]].getCanonicalName
  private val linkedListClassName = classOf[JList[_]].getCanonicalName
  private val richWindowFunction = classOf[RichWindowFunction[_, _, _, _]].getCanonicalName
  private val richAllWindowFunction = classOf[RichAllWindowFunction[_, _, _]].getCanonicalName

  def genOpen(
      isTimeWindowFunction: Boolean,
      properties: Seq[NamedWindowProperty],
      finalRowArity: Int): String = {

    if (isTimeWindowFunction) {
      val (startPos, endPos, timePos) = computeWindowPropertyPos(properties)
      j"""
         |   @Override
         |   public void open(${classOf[Configuration].getCanonicalName} parameters) {
         |     output = new $cRowClassName(new $rowClassName($finalRowArity), true);
         |     collector = new $timeCollectorClassName(
         |       scala.Option.apply(${if (startPos.isDefined) startPos.get else "null"}),
         |       scala.Option.apply(${if (endPos.isDefined) endPos.get else "null"}),
         |       scala.Option.apply(${if (timePos.isDefined) timePos.get else "null"}));
         |   }
          """.stripMargin
    } else {
      j"""
         |   @Override
         |   public void open(${classOf[Configuration].getCanonicalName} parameters) {
         |     output = new $cRowClassName(new $rowClassName($finalRowArity), true);
         |   }
          """.stripMargin
    }
  }

  def genApply(
      isGroupWindowFunction: Boolean,
      isTimeWindowFunction: Boolean,
      numGroupingKey: Int): String = {

    val setCollector: String =
      j"""
          |   collector.wrappedCollector_$$eq(out);
          |   collector.windowStart_$$eq((($timeWindowClassName)window).getStart());
          |   collector.windowEnd_$$eq((($timeWindowClassName)window).getEnd());
        """.stripMargin

    val setGroupKey: String =
      j"""
          |   for (int i = 0; i < $numGroupingKey; ++i) {
          |       output.row().setField(i, (($rowClassName)key).getField(i));
          |   }
          |
      """.stripMargin

    j"""
        |    @Override
        |    public void apply(
        |        ${if (isGroupWindowFunction) "Object key,"  else ""}
        |        $windowClassName window,
        |        $javaIterableClassName<$linkedListClassName<$rowClassName>> records,
        |        ${classOf[Collector[CRow]].getCanonicalName}<$cRowClassName> out)
        |    throws Exception {
        |
        |        ${if (isTimeWindowFunction) setCollector else ""}
        |
        |        for ($linkedListClassName<$rowClassName> list: records) {
        |             ${if (isGroupWindowFunction) setGroupKey else ""}
        |             for ($rowClassName record: list) {
        |                  for (int i = 0; i < record.getArity(); i++) {
        |                      output.row().setField(
        |                      ${if (isGroupWindowFunction) numGroupingKey + " + " else ""} i,
        |                      record.getField(i));
        |                  }
        |                  ${if (isTimeWindowFunction) "collector" else "out"}.collect(output);
        |             }
        |        }
        |    }
        |
        |
        """.stripMargin
  }

  def generateAggregateAllWindowFunction(
      name: String,
      finalRowArity: Int): GeneratedWindowFunction = {

    val funcName = newName(name)
    val funcCode = j"""
      public class $funcName
        extends ${richAllWindowFunction}<$linkedListClassName<$rowClassName>, $cRowClassName,
                $windowClassName> {

        private $cRowClassName output;
        ${genOpen(false, null, finalRowArity)}
        ${genApply(false, false, 0)}
      }
    """.stripMargin

    GeneratedWindowFunction(funcName, funcCode)
  }

  def generateAggregateAllTimeWindowFunction(
      name: String,
      properties: Seq[NamedWindowProperty],
      finalRowArity: Int): GeneratedWindowFunction = {

    val funcName = newName(name)
    val funcCode = j"""
      public class $funcName
        extends ${richAllWindowFunction}<$linkedListClassName<$rowClassName>, $cRowClassName,
                $timeWindowClassName> {

        private $cRowClassName output;
        private $timeCollectorClassName collector;

        ${genOpen(true, properties, finalRowArity)}
        ${genApply(false, true, 0)}
      }
    """.stripMargin

    GeneratedWindowFunction(funcName, funcCode)
  }

  def generateAggregateWindowFunction(
      name: String,
      numGroupingKey: Int,
      aggOutputArity: Int,
      finalRowArity: Int): GeneratedWindowFunction = {

    val funcName = newName(name)
    val funcCode = j"""
      public class $funcName
        extends ${richWindowFunction}<$linkedListClassName<$rowClassName>, $cRowClassName,
                $rowClassName, $windowClassName> {

        private $cRowClassName output;

        ${genOpen(false, null, finalRowArity)}
        ${genApply(true, false, numGroupingKey)}
      }
    """.stripMargin

    GeneratedWindowFunction(funcName, funcCode)
  }

  def generateAggregateTimeWindowFunction(
      name: String,
      numGroupingKey: Int,
      aggOutputArity: Int,
      properties: Seq[NamedWindowProperty],
      finalRowArity: Int): GeneratedWindowFunction = {

    val funcName = newName(name)
    val funcCode = j"""
      public class $funcName
        extends ${richWindowFunction}<$linkedListClassName<$rowClassName>, $cRowClassName,
                $rowClassName, timeWindowClassName> {

        private $cRowClassName output;
        private $timeCollectorClassName collector;

        ${genOpen(true, properties, finalRowArity)}
        ${genApply(true, true, numGroupingKey)}
      }
    """.stripMargin

    GeneratedWindowFunction(funcName, funcCode)
  }
}

