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

import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.types.Row

import java.util.{LinkedList => JList}

/**
  * Aggregate Function used for the aggregate operator in
  * [[org.apache.flink.streaming.api.datastream.WindowedStream]]
  *
  * @param genAggregations Generated aggregate helper function
  */
class AggregateAggFunction[F <: GeneratedAggregations](
    genAggregations: GeneratedAggregationsFunction)
  extends AggregateAggFunctionBase[JList[Row], F](genAggregations) {


  override def getResult(accumulatorRow: Row): JList[Row] = {
    if (function == null) {
      initFunction()
    }
    val output = function.createOutputRow()
    function.setAggregationResults(accumulatorRow, output)
    val list = new JList[Row]()
    list.add(output)
    list
  }
}
