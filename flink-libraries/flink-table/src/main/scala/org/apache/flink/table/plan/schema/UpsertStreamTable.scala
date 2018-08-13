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

package org.apache.flink.table.plan.schema

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Boolean => JBool}

import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.plan.stats.FlinkStatistic

class UpsertStreamTable[T](
    val dataStream: DataStream[JTuple2[JBool, T]],
    val uniqueKeys: Array[String],
    override val fieldIndexes: Array[Int],
    override val fieldNames: Array[String],
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends InlineTable[T](
    dataStream.getType.asInstanceOf[TupleTypeInfo[JTuple2[JBool, T]]].getTypeAt(1),
    fieldIndexes,
    fieldNames,
    statistic)
