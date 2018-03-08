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
package org.apache.flink.table.runtime.join

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Connect data for left stream and right stream. Only use for left or right join without
  * NonEquiPredicates.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param resultType      the output type of join
  * @param genJoinFuncName the function code of other non-equi condition
  * @param genJoinFuncCode the function name of other non-equi condition
  * @param isLeftJoin      the type of join
  * @param queryConfig     the configuration for the query to generate
  */
abstract class NonWindowOuterJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    resultType: TypeInformation[CRow],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    isLeftJoin: Boolean,
    queryConfig: StreamQueryConfig)
  extends NonWindowJoin(
    leftType,
    rightType,
    resultType,
    genJoinFuncName,
    genJoinFuncCode,
    queryConfig) {

  // result row, all fields from right will be null
  protected var leftResultRow: Row = _
  protected var rightResultRow: Row = _
  // how many matched rows from the right table for each left row
  protected var joinCntState: Array[MapState[Row, Long]] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    leftResultRow = new Row(resultType.getArity)
    rightResultRow = new Row(resultType.getArity)

    joinCntState = new Array[MapState[Row, Long]](2)
    val leftJoinCntStateDescriptor = new MapStateDescriptor[Row, Long](
      "leftJoinCnt", leftType, Types.LONG.asInstanceOf[TypeInformation[Long]])
    joinCntState(0) = getRuntimeContext.getMapState(leftJoinCntStateDescriptor)
    val rightJoinCntStateDescriptor = new MapStateDescriptor[Row, Long](
      "rightJoinCnt", rightType, Types.LONG.asInstanceOf[TypeInformation[Long]])
    joinCntState(1) = getRuntimeContext.getMapState(rightJoinCntStateDescriptor)

    LOG.debug(s"Instantiating NonWindowOuterJoin")
  }


  def normalJoin(
      inputRow: Row,
      inputRowFromLeft: Boolean,
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      curProcessTime: Long
  ): Long = {
    val otherSideIterator = otherSideState.iterator()
    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue
      // join
      cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
      // clear expired data. Note: clear after join to keep closer to the original semantics
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
    val joinCnt = cRowWrapper.getEmitCnt
    // The result is NULL from the right side, if there is no match.
    if (joinCnt == 0) {
      cRowWrapper.setTimes(1)
      collectWithDefaultValue(inputRow, inputRowFromLeft, cRowWrapper)
    }
    joinCnt
  }

  def retractJoin(
      value: CRow,
      inputRowFromLeft: Boolean,
      currentSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      curProcessTime: Long): Unit = {

    val inputRow = value.row
    val otherSideIterator = otherSideState.iterator()
    // number of record in current side, here we only check whether number equals to 0 or 1.
    val recordNum: Long = recordNumInState(currentSideState)

    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue
      cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)

      // retract previous record with null right
      if (recordNum == 1 && value.change) {
        cRowWrapper.setChange(false)
        collectWithDefaultValue(otherSideRow, !inputRowFromLeft, cRowWrapper)
        cRowWrapper.setChange(true)
      }
      // do normal join
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)

      // output with null right if have to
      if (!value.change && recordNum == 0) {
        cRowWrapper.setChange(true)
        collectWithDefaultValue(otherSideRow, !inputRowFromLeft, cRowWrapper)
        cRowWrapper.setChange(false)
      }
      // clear expired data. Note: clear after join to keep closer to the original semantics
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
  }


  def callJoinFunction(
      inputRow: Row,
      inputRowFromLeft: Boolean,
      otherSideRow: Row,
      cRowWrapper: Collector[Row]): Unit = {

    if (inputRowFromLeft) {
      joinFunction.join(inputRow, otherSideRow, cRowWrapper)
    } else {
      joinFunction.join(otherSideRow, inputRow, cRowWrapper)
    }
  }
  
  /**
    * Return number of records in corresponding state. Only return 0, 1, 2 because only these
    * numbers matter.
    */
  def recordNumInState(currentSideState: MapState[Row, JTuple2[Long, Long]]): Long = {
    var recordNum = 0L
    val it = currentSideState.iterator()
    while(it.hasNext && recordNum < 2) {
      val entry = it.next()
      recordNum += entry.getValue.f0
    }
    recordNum
  }

  /**
    * Connect input row with default null value if there is no match, then collect.
    *
    * @param inputRow         The input row.
    * @param inputFromLeft
    * @param out             The collector for returning result values.
    */
  def collectWithDefaultValue(
      inputRow: Row,
      inputFromLeft: Boolean,
      out: Collector[Row]): Unit = {

    var i = 0
    if (inputFromLeft) {
      while (i < inputRow.getArity) {
        leftResultRow.setField(i, inputRow.getField(i))
        i += 1
      }
      out.collect(leftResultRow)
    } else {
      while (i < inputRow.getArity) {
        val idx = rightResultRow.getArity - inputRow.getArity + i
        rightResultRow.setField(idx, inputRow.getField(i))
        i += 1
      }
      out.collect(rightResultRow)
    }
  }

  /**
    * Removes records which are expired from left state. Registers a new timer if the state still
    * holds records after the clean-up. Also, clear leftJoinCnt map state when clear left
    * rowMapState.
    */
  def expireOutTimeRow(
      curTime: Long,
      rowMapState: MapState[Row, JTuple2[Long, Long]],
      timerState: ValueState[Long],
      isLeft: Boolean,
      joinCntState: Array[MapState[Row, Long]],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    val joinCntIdx = getJoinCntIndex(isLeft)
    val rowMapIter = rowMapState.iterator()
    var validTimestamp: Boolean = false

    while (rowMapIter.hasNext) {
      val mapEntry = rowMapIter.next()
      val recordExpiredTime = mapEntry.getValue.f1
      if (recordExpiredTime <= curTime) {
        rowMapIter.remove()
        joinCntState(joinCntIdx).remove(mapEntry.getKey)
      } else {
        // we found a timestamp that is still valid
        validTimestamp = true
      }
    }
    // If the state has non-expired timestamps, register a new timer.
    // Otherwise clean the complete state for this input.
    if (validTimestamp) {
      val cleanupTime = curTime + maxRetentionTime
      ctx.timerService.registerProcessingTimeTimer(cleanupTime)
      timerState.update(cleanupTime)
    } else {
      timerState.clear()
      rowMapState.clear()
      if (isLeft == isLeftJoin) {
        joinCntState(joinCntIdx).clear()
      }
    }
  }

  def getJoinCntIndex(isInputFromLeft: Boolean): Int = {
    if (isInputFromLeft) {
      0
    } else {
      1
    }
  }

  def retractJoinWithNonEquiPreds(
      value: CRow,
      inputRowFromLeft: Boolean,
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      joinCntIdx: Int,
      curProcessTime: Long
  ): Unit = {

    val inputRow = value.row
    val otherSideIterator = otherSideState.iterator()
    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue

      cRowWrapper.setLazyOutput(true)
      cRowWrapper.setRow(null)
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
      cRowWrapper.setLazyOutput(false)
      if (cRowWrapper.getRow() != null) {
        cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
        val joinCnt = joinCntState(1 - joinCntIdx).get(otherSideRow)
        if (value.change) {
          joinCntState(1 - joinCntIdx).put(otherSideRow, joinCnt + 1L)
          if (joinCnt == 0) {
            // retract previous non matched result row
            cRowWrapper.setChange(false)
            collectWithDefaultValue(otherSideRow, !inputRowFromLeft, cRowWrapper)
            cRowWrapper.setChange(true)
          }
          // do normal join
          callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
        } else {
          joinCntState(1 - joinCntIdx).put(otherSideRow, joinCnt - 1L)
          // do normal join
          callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
          if (joinCnt == 1) {
            // output non matched result row
            cRowWrapper.setChange(true)
            collectWithDefaultValue(otherSideRow, !inputRowFromLeft, cRowWrapper)
            cRowWrapper.setChange(false)
          }
        }
      }
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
  }
}

