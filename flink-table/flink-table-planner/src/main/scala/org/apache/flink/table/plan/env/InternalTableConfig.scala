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
package org.apache.flink.table.plan.env

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.CalciteConfig

/**
 * A config to define the runtime behavior of the Table API.
 */
class InternalTableConfig(tableConfig: TableConfig) extends TableConfig {

  def this() = {
    this(new TableConfig())
  }

  setTimeZone(tableConfig.getTimeZone)

  setNullCheck(tableConfig.getNullCheck)

  setDecimalContext(tableConfig.getDecimalContext)

  setMaxGeneratedCodeLength(tableConfig.getMaxGeneratedCodeLength)

  /**
    * Defines the configuration of Calcite for Table API and SQL queries.
    */
  private var calciteConfig: CalciteConfig = CalciteConfig.DEFAULT

  /**
    * Returns the current configuration of Calcite for Table API and SQL queries.
    */
  def getCalciteConfig: CalciteConfig = calciteConfig

  /**
    * Sets the configuration of Calcite for Table API and SQL queries.
    * Changing the configuration has no effect after the first query has been defined.
    */
  def setCalciteConfig(calciteConfig: CalciteConfig): Unit = {
    this.calciteConfig = calciteConfig
  }
}

object InternalTableConfig {
  def DEFAULT = new InternalTableConfig()
}
