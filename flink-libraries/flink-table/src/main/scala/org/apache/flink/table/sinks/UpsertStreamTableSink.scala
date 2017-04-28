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

package org.apache.flink.table.sinks

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{Table, Types}

/**
  * Defines an external [[TableSink]] to emit a streaming [[Table]] with insert, update, and delete
  * changes. The [[Table]] must be have a unique key (atomic or composite).
  *
  * If the [[Table]] does not have a unique key, a [[org.apache.flink.table.api.TableException]]
  * will be thrown.
  *
  * The [[Table]] will be converted into a stream of upsert and delete messages which are encoded as
  * [[JTuple2]]. The first field is a [[JBool]] flag to indicate the message type. The second field
  * holds the record of the requested type [[T]].
  *
  * A message with true [[JBool]] field is an upsert message for the specified key.
  * A message with false flag is a delete message for the specified key.
  *
  * @tparam T Type of records that this [[TableSink]] expects and supports.
  */
trait UpsertStreamTableSink[T] extends TableSink[JTuple2[JBool, T]] {

  /**
    * Sets the keys of the [[Table]].
    * The method is called after [[TableSink.configure()]].
    */
  def setKeyFields(keys: Array[String]): Unit

  /** Returns the requested record type */
  def getRecordType: TypeInformation[T]

  /** Emits the DataStream. */
  def emitDataStream(dataStream: DataStream[JTuple2[JBool, T]]): Unit

  override def getOutputType = new TupleTypeInfo(Types.BOOLEAN, getRecordType)
}
