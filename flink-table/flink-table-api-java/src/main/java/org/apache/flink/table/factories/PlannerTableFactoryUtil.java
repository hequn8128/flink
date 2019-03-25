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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.lang.reflect.Constructor;

/**
 * Utility for dealing with {@link TableFactory} using the {@code TableFactoryService}.
 */
public interface PlannerTableFactoryUtil {

	static PlannerTableFactoryUtil create() {
		return SingletonPlannerTableFactoryUtil.getTableFactoryUtil();
	}
	/**
	 * Returns a table source for a table environment.
	 */
	<T> TableSource<T> findAndCreateTableSource(TableEnvironment tableEnvironment, Descriptor descriptor);

	/**
	 * Returns a table sink for a table environment.
	 */
	<T> TableSink<T> findAndCreateTableSink(TableEnvironment tableEnvironment, Descriptor descriptor);

	/**
	 * Util class to create {@link PlannerTableFactoryUtil} instance. Use singleton pattern to avoid
	 * creating many {@link PlannerTableFactoryUtil}.
	 */
	class SingletonPlannerTableFactoryUtil {

		private static volatile PlannerTableFactoryUtil tableFactoryUtil;

		private SingletonPlannerTableFactoryUtil() {}

		public static PlannerTableFactoryUtil getTableFactoryUtil() {

			if (tableFactoryUtil == null) {
				try {
					Class<?> clazz = Class.forName("org.apache.flink.table.factories.PlannerTableFactoryUtilImpl");
					Constructor<?> con = clazz.getConstructor();
					tableFactoryUtil = (PlannerTableFactoryUtil) con.newInstance();
				} catch (Throwable t) {
					throw new TableException("Construction of PlannerTableFactoryUtilImpl class failed.", t);
				}
			}
			return tableFactoryUtil;
		}
	}
}
