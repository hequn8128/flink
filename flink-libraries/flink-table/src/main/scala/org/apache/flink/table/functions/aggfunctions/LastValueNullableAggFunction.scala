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
package org.apache.flink.table.functions.aggfunctions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for LastValue(nullable) aggregate function */
class LastValueNullableAccumulator[T] extends JTuple1[T]

/**
  * Base class for built-in LastValue(nullable) aggregate function
  *
  * @tparam T the type for the aggregation result
  */
class LastValueNullableAggFunction[T](var valueTypeInfo: TypeInformation[_])
  extends AggregateFunction[T, LastValueNullableAccumulator[T]] {

  override def createAccumulator(): LastValueNullableAccumulator[T] = {
    val acc = new LastValueNullableAccumulator[T]
    acc.f0 = getInitValue
    acc
  }

  def accumulate(acc: LastValueNullableAccumulator[T], value: Any): Unit = {
    acc.f0 = value.asInstanceOf[T]
  }

  override def getValue(acc: LastValueNullableAccumulator[T]): T = {
    acc.f0
  }

  def resetAccumulator(acc: LastValueNullableAccumulator[T]): Unit = {
    acc.f0 = getInitValue
  }

  override def getAccumulatorType: TypeInformation[LastValueNullableAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[LastValueNullableAccumulator[T]],
      getValueTypeInfo)
  }

  def getInitValue: T = null.asInstanceOf[T]

  def getValueTypeInfo: TypeInformation[_] = {
    valueTypeInfo
  }
}
