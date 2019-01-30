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

package org.apache.flink.table.api

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
class TablePlanner(val config: TableConfig) {

  def createTableEnvironment[ENV <: org.apache.flink.table.api.TableEnvironment](
      language: String): ENV = {

    language match {
      case "scala" =>
        val env = ScalaStreamExecEnv.getExecutionEnvironment
        new StreamTableEnvironment(env, config).asInstanceOf[ENV]
      case "java" => null.asInstanceOf[ENV]
    }
  }
}


