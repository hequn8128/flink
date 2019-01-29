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

package org.apache.flink.table.api.java

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.{TableConfig, TablePlannerFactory}
import org.apache.flink.table.catalog.ExternalCatalog
import org.apache.flink.table.factories.TablePlannerUtil
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

import _root_.scala.annotation.varargs

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
trait TableEnvironment extends org.apache.flink.table.api.TableEnvironment {

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T](name: String, tf: TableFunction[T]): Unit

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T, ACC](name: String, f: AggregateFunction[T, ACC]): Unit

}


object TableEnvironment {

  def create(tableConfig: TableConfig): org.apache.flink.table.api.java.TableEnvironment = {
    TablePlannerUtil
      .find(classOf[TablePlannerFactory], TablePlannerUtil.generatePlannerDiscriptor(tableConfig))
      .createTablePlanner(tableConfig)
      .createTableEnvironment[org.apache.flink.table.api.java.TableEnvironment]("java")
  }
}



