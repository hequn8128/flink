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

package org.apache.flink.table.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

/**
 * Doc.
 */
public interface TableEnvironment {

	Table fromTableSource(TableSource<?> source);

	void registerExternalCatalog(String name, ExternalCatalog externalCatalog);

	ExternalCatalog getRegisteredExternalCatalog(String name);

	void registerFunction(String name, ScalarFunction function);

	void registerTable(String name, Table table);

	void registerTableSource(String name, TableSource<?> tableSource);

	void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink);

	void registerTableSink(String name, TableSink<?> configuredSink);

	Table scan(String... tablePath);

	TableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	String[] listTables();

	String[] listUserDefinedFunctions();

	String explain(Table table);

	String[] getCompletionHints(String statement, int position);

	Table sqlQuery(String query);

	void sqlUpdate(String stmt);

	void sqlUpdate(String stmt, QueryConfig config);

	QueryConfig queryConfig();

	TableConfig getConfig();


}
