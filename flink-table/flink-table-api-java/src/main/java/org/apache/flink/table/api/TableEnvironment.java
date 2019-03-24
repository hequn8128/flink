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
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.IntStream;

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

	/**
	 * Returns field names for a given [[TypeInformation]].
	 *
	 * @param inputType The TypeInformation extract the field names.
	 * @tparam A The type of the TypeInformation.
	 * @return An array holding the field names
	 */
	static <A> String[] getFieldNames(TypeInformation<A> inputType) {
		validateType(inputType);

		String[] fieldNames;
		if (inputType instanceof CompositeType<?>) {
			fieldNames = ((CompositeType<?>) inputType).getFieldNames();
		} else {
			fieldNames = new String[]{"f0"};
		}

		if (Arrays.stream(fieldNames).anyMatch("*"::equals)) {
			throw new TableException("Field name can not be '*'.");
		}

		return fieldNames;
	}

	/**
	 * Validate if class represented by the typeInfo is static and globally accessible.
	 * @param typeInfo type to check
	 * @throws TableException if type does not meet these criteria
	 */
	static void validateType(TypeInformation<?> typeInfo) {
		Class<?> clazz = typeInfo.getTypeClass();
		if ((clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())) ||
			!Modifier.isPublic(clazz.getModifiers()) ||
			clazz.getCanonicalName() == null) {
			throw new TableException(String.format(
				"Class '%s' described in type information '%s' must be static and globally " +
					"accessible.", clazz, typeInfo));
		}
	}

	/**
	 * Returns field indexes for a given [[TypeInformation]].
	 *
	 * @param inputType The TypeInformation extract the field positions from.
	 * @return An array holding the field positions
	 */
	static int[] getFieldIndices(TypeInformation<?> inputType) {
		return IntStream.range(0, getFieldNames(inputType).length).toArray();
	}

	/**
	 * Returns field types for a given [[TypeInformation]].
	 *
	 * @param inputType The TypeInformation to extract field types from.
	 * @return An array holding the field types.
	 */
	static TypeInformation<?>[] getFieldTypes(TypeInformation<?> inputType) {
		validateType(inputType);

		TypeInformation<?>[] fieldTypes;
		if (inputType instanceof CompositeType<?>) {
			CompositeType<?> compositeType = (CompositeType<?>) inputType;
			fieldTypes = (TypeInformation<?>[]) IntStream.range(0, compositeType.getArity())
				.mapToObj(i -> compositeType.getTypeAt(i)).toArray();
		} else {
			fieldTypes = new TypeInformation<?>[]{inputType};
		}
		return fieldTypes;
	}
}
