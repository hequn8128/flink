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

import org.apache.flink.table.codegen.{GeneratedAggregationsFunction}
import org.apache.flink.table.utils.RetractableCollector
import org.apache.flink.types.Row
import java.util.{LinkedList => JList}

/**
  * Table Aggregate Function used for the aggregate operator in
  * [[org.apache.flink.streaming.api.datastream.WindowedStream]].
  *
  * @param genAggregations Generated aggregate helper function
  */
class TableAggregateAggFunction[F <: GeneratedTableAggregations](
    genAggregations: GeneratedAggregationsFunction)
  extends AggregateAggFunctionBase[JList[Row], F](genAggregations) {

  override def getResult(accumulatorRow: Row): JList[Row] = {
    if (function == null) {
      initFunction()
    }

    // new buffer collector
    val bufferCollector = new BufferedCollector()
    bufferCollector.init()
    // Output data into a memory buffer.
    // There is no way to emit data in [[org.apache.flink.api.common.functions.AggregateFunction]],
    // we have to buffer the data in a list and return to the WindowFunction.
    function.emit(accumulatorRow, bufferCollector)
    bufferCollector.getResults
  }

  /**
    * Collect data into a memory buffer.
    */
  private class BufferedCollector extends RetractableCollector[Row] {

    private var list: JList[Row] = _

    def init(): Unit = {
      list = new JList[Row]
    }

    def getResults(): JList[Row] = list

    /**
      * Emits a record into the buffer.
      */
    override def collect(record: Row): Unit = {
      // can't reuse here, need to copy
      list.add(Row.copy(record))
    }

    override def retract(record: Row): Unit = {
      throw new RuntimeException("TableAggreagateFunction can't emit retractions in Window.")
    }

    override def close(): Unit = {
      list.clear()
    }
  }
}
