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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.table.api.dataview.ListView;

/**
 * {@link ListView} type information.
 *
 * @param <T> The type of the elements in the {@link ListView}.
 */
public class ListViewTypeInfo<T> extends TypeInformation<ListView<T>> {

	private final TypeInformation<T> elementType;

	public ListViewTypeInfo(TypeInformation<T> elementType) {
		this.elementType = elementType;
	}

	/**
	 * Gets the type information for the elements contained in the {@link ListView}.
	 */
	public TypeInformation<T> getElementType() {
		return elementType;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<ListView<T>> getTypeClass() {
		return (Class<ListView<T>>) (Class<?>) ListView.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<ListView<T>> createSerializer(ExecutionConfig config) {
		TypeSerializer<T> typeSer = elementType.createSerializer(config);
		return new ListViewSerializer<T>(new ListSerializer<T>(typeSer));
	}

	@Override
	public String toString() {
		return "ListView<" + elementType.toString() + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj instanceof ListViewTypeInfo) {
			final ListViewTypeInfo<?> other = (ListViewTypeInfo<?>) obj;
			return other.canEqual(this) && elementType.equals(other.elementType);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * elementType.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}
}
