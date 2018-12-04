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

package org.apache.flink.table.runtime

import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.RetractableCollector
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * The collector is used to wrap a [[Row]] to a [[CRow]] and is retractable.
  */
class CRowWrappingRetractableCollector extends RetractableCollector[Row] {

  var out: Collector[CRow] = _
  val outCRow: CRow = new CRow()

  override def collect(record: Row): Unit = {
    outCRow.change = true
    outCRow.row = record
    out.collect(outCRow)
  }

  override def retract(record: Row): Unit = {
    outCRow.change = false
    outCRow.row = record
    out.collect(outCRow)
  }

  override def close(): Unit = out.close()
}
