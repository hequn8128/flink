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

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{Compiler, GeneratedTableAggregationsFunction}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * [[RichGroupReduceFunction]] to compute the final result of a pre-aggregated table aggregation
  * for batch (DataSet) queries.
  *
  * @param genAggregations Code-generated [[GeneratedTableAggregations]]
  */
class DataSetFinalTableAggFunction(
    private val genAggregations: GeneratedTableAggregationsFunction)
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedTableAggregations] with Logging {

  private var accumulators: Row = _

  private var function: GeneratedTableAggregations = _
  private var collector: BatchRetractableCollector = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling TableAggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating TableAggregateHelper.")
    function = clazz.newInstance()

    accumulators = function.createAccumulators()
    collector = new BatchRetractableCollector
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    val iterator = records.iterator()

    // reset first accumulator
    function.resetAccumulator(accumulators)

    var record: Row = null
    while (iterator.hasNext) {
      record = iterator.next()
      // accumulate
      function.mergeAccumulatorsPair(accumulators, record)
    }

    collector.out = out
    function.emit(accumulators, collector)
  }
}
