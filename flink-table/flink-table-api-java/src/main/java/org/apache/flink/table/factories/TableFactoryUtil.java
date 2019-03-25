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
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

/**
 * Utility for dealing with {@link TableFactory} using the {@code TableFactoryService}.
 */
public final class TableFactoryUtil {

	/**
	 * Returns a table source for a table environment.
	 */
	public static <T> TableSource<T> findAndCreateTableSource(TableEnvironment tableEnvironment, Descriptor descriptor) {
		return PlannerTableFactoryUtil.create().findAndCreateTableSource(tableEnvironment, descriptor);
	}

	/**
	 * Returns a table sink for a table environment.
	 */
	public static <T> TableSink<T> findAndCreateTableSink(TableEnvironment tableEnvironment, Descriptor descriptor) {
		return PlannerTableFactoryUtil.create().findAndCreateTableSink(tableEnvironment, descriptor);
	}
}
