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
package org.apache.flink.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.table.codegen.{Compiler, GeneratedWindowFunction}
import org.apache.flink.table.util.Logging
import java.util.{List => JList}

/**
  * Computes the final aggregate value from incrementally computed aggregates.
  *
  * @param generatedWindowFunction The Code-generated [[GeneratedWindowFunction]].
  */
class IncrementalAggregateAllWindowFunction[W <: Window](
  generatedWindowFunction: GeneratedWindowFunction)
  extends RichAllWindowFunction[JList[Row], CRow, W]
    with Compiler[RichAllWindowFunction[JList[Row], CRow, W]]
    with Logging {

  protected var function: RichAllWindowFunction[JList[Row], CRow, W] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling AllWindowFunctionHelper: $generatedWindowFunction.name \n\n " +
      s"Code:\n$generatedWindowFunction.code")
    val clazz = compile(
      Thread.currentThread().getContextClassLoader,
      generatedWindowFunction.name,
      generatedWindowFunction.code)
    LOG.debug("Instantiating AllWindowFunctionHelper.")
    function = clazz.newInstance()

    function.open(parameters)
  }

  /**
    * Calculate aggregated values output by aggregate buffer, and set them into output
    * Row based on the mapping relation between intermediate aggregate data and output data.
    */
  override def apply(
    window: W,
    records: Iterable[JList[Row]],
    out: Collector[CRow]): Unit = {
    function.apply(window, records, out)
  }
}
