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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.lang.reflect.Constructor;

/**
 * Doc.
 */
public interface StreamTableEnvironment extends TableEnvironment {

	<T> void registerFunction(String name, TableFunction<T> tableFunction);

	<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);

	StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	<T> Table fromDataStream(DataStream<T> dataStream);

	<T> Table fromDataStream(DataStream<T> dataStream, String fields);

	<T> void registerDataStream(String name, DataStream<T> dataStream);

	<T> void registerDataStream(String name, DataStream<T> dataStream, String fields);

	<T> DataStream<T> toAppendStream(Table table, Class<T> clazz);

	<T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo);

	<T> DataStream<T> toAppendStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig);

	<T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig);

	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz);

	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo);

	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz, StreamQueryConfig queryConfig);

	<T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo, StreamQueryConfig queryConfig);

	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {
		return create(executionEnvironment, TableConfig.DEFAULT);
	}

	static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment, TableConfig tableConfig) {
		try {
			Class clazz = Class.forName("org.apache.flink.table.plan.env.java.StreamTableEnvImpl");
			Constructor con = clazz.getConstructor(StreamExecutionEnvironment.class, TableConfig.class);
			return (StreamTableEnvironment) con.newInstance(executionEnvironment, tableConfig);
		} catch (Throwable t) {
			throw new RuntimeException("Create StreamTableEnvironment failed.");
		}
	}
}
