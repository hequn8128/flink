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

package org.apache.flink.table.api.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.lang.reflect.Constructor;

/**
 * Doc.
 */
public interface BatchTableEnvironment extends TableEnvironment {

	<T> void registerFunction(String name, TableFunction<T> tableFunction);

	<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);

	<T> Table fromDataSet(DataSet<T> dataSet);

	<T> Table fromDataSet(DataSet<T> dataSet, String fields);

	<T> void registerDataSet(String name, DataSet<T> dataSet);

	<T> void registerDataSet(String name, DataSet<T> dataSet, String fields);

	<T> DataSet<T> toDataSet(Table table, Class<T> clazz);

	<T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo);

	<T> DataSet<T> toDataSet(Table table, Class<T> clazz, BatchQueryConfig queryConfig);

	<T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo, BatchQueryConfig queryConfig);

	static BatchTableEnvironment create(ExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, new TableConfig());
	}

	static BatchTableEnvironment create(ExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		try {
			Class clazz = Class.forName("org.apache.flink.table.api.java.BatchTableEnvImpl");
			Constructor con = clazz.getConstructor(ExecutionEnvironment.class, TableConfig.class);
			return (BatchTableEnvironment) con.newInstance(executionEnvironment, tableConfig);
		} catch (Throwable t) {
			throw new TableException("Create BatchTableEnvImpl failed.", t);
		}
	}
}
