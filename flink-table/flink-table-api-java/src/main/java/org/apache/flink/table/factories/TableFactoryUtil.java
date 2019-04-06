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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Utility for dealing with {@link TableFactory} using the {@link TableFactoryService}.
 */
public class TableFactoryUtil {

	/**
	 * Returns an external catalog.
	 */
	public static ExternalCatalog findAndCreateExternalCatalog(Descriptor descriptor) {
		Map<String, String> properties = descriptor.toProperties();
		return TableFactoryService
			.find(ExternalCatalogFactory.class, properties)
			.createExternalCatalog(properties);
	}

	/**
	 * Returns a table source matching the descriptor.
	 */
	public static <T> TableSource<T> findAndCreateTableSource(Descriptor descriptor) {
		Map<String, String> properties = descriptor.toProperties();

		TableSource<T> tableSource;
		try {
			if (descriptor instanceof BatchTableDescriptor || isBatchExternalCatalogTable(descriptor)) {

				Object object = TableFactoryService.find(
					Class.forName("org.apache.flink.table.factories.BatchTableSourceFactory"),
					properties);
				Method method = object.getClass().getDeclaredMethod("createBatchTableSource", Map.class);

				tableSource = (TableSource<T>) method.invoke(object, properties);
			} else if (descriptor instanceof StreamTableDescriptor || isStreamExternalCatalogTable(descriptor)) {

				Object object = TableFactoryService.find(
					Class.forName("org.apache.flink.table.factories.StreamTableSourceFactory"),
					properties);
				Method method = object.getClass().getDeclaredMethod("createStreamTableSource", Map.class);

				tableSource = (TableSource<T>) method.invoke(object, properties);
			} else {
				throw new TableException(
					String.format(
						"Unsupported table descriptor: %s",
						descriptor.getClass().getName())
				);
			}
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSource failed.", t);
		}

		return tableSource;
	}

	/**
	 * Returns a table sink matching the descriptor.
	 */
	public static <T> TableSink<T> findAndCreateTableSink(Descriptor descriptor) {
		Map<String, String> properties = descriptor.toProperties();

		TableSink<T> tableSink;
		try {
			if (descriptor instanceof BatchTableDescriptor || isBatchExternalCatalogTable(descriptor)) {
				Object object = TableFactoryService.find(
					Class.forName("org.apache.flink.table.factories.BatchTableSinkFactory"),
					properties);
				Method method = object.getClass().getDeclaredMethod("createBatchTableSink", Map.class);

				tableSink = (TableSink<T>) method.invoke(object, properties);
			} else if (descriptor instanceof StreamTableDescriptor || isStreamExternalCatalogTable(descriptor)) {
				Object object = TableFactoryService.find(
					Class.forName("org.apache.flink.table.factories.StreamTableSinkFactory"),
					properties);
				Method method = object.getClass().getDeclaredMethod("createStreamTableSink", Map.class);

				tableSink = (TableSink<T>) method.invoke(object, properties);
			} else {
				throw new TableException(
					String.format(
						"Unsupported table descriptor: %s",
						descriptor.getClass().getName())
				);
			}
		} catch (Throwable t) {
			throw new TableException("findAndCreateTableSink failed.", t);
		}

		return tableSink;
	}

	/**
	 * Return true if the descriptor is an ExternalCatalogTable intended for stream environments.
	 */
	private static boolean isStreamExternalCatalogTable(Descriptor descriptor) {
		return descriptor instanceof ExternalCatalogTable &&
			((ExternalCatalogTable) descriptor).isStreamTable();
	}

	/**
	 * Return true if the descriptor is an ExternalCatalogTable intended for batch environments.
	 */
	private static boolean isBatchExternalCatalogTable(Descriptor descriptor) {
		return descriptor instanceof ExternalCatalogTable &&
			((ExternalCatalogTable) descriptor).isBatchTable();
	}
}
