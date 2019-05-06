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

import org.apache.flink.types.Row

/**
  * The collector is used to concat group key and table aggregate function output.
  */
class ConcatKeyAndAggResultCollector(val keyNum: Int) extends CRowWrappingCollector {

  var resultRow: Row = _

  def setResultRow(row: Row): Unit = {
    resultRow = row
  }

  def getResultRow: Row = {
    resultRow
  }

  override def collect(record: Row): Unit = {
    var i = 0
    val offset = keyNum
    while (i < record.getArity) {
      resultRow.setField(i + offset, record.getField(i))
      i += 1
    }
    super.collect(resultRow)
  }
}
