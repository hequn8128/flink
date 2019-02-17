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

import org.apache.flink.api.common.typeutils.*;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.ListView;

import java.io.IOException;
import java.util.List;

/**
 * A serializer for {@link ListView}. The serializer relies on an element
 * serializer for the serialization of the list's elements.
 *
 * <p>The serialization format for the list is as follows: four bytes for the length of the list,
 * followed by the serialized representation of each element.
 *
 * @param <T> The type of element in the list.
 */
public class ListViewSerializer<T> extends TypeSerializer<ListView<T>> {

	private static final long serialVersionUID = -2030398712359267867L;

	/** The serializer for the elements of the list. */
	private final TypeSerializer<List<T>> listSerializer;

	public ListViewSerializer(TypeSerializer<List<T>> elementSerializer) {
		this.listSerializer = elementSerializer;
	}

	public TypeSerializer<List<T>> getListSerializer() {
		return listSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<ListView<T>> duplicate() {
		return new ListViewSerializer<T>(listSerializer.duplicate());
	}

	@Override
	public ListView<T> createInstance() {
		return new ListView<>();
	}

	@Override
	public ListView<T> copy(ListView<T> from) {
		return new ListView<>(null, listSerializer.copy(from.getList()));
	}

	@Override
	public ListView<T> copy(ListView<T> from, ListView<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ListView<T> record, DataOutputView target) throws IOException {
		listSerializer.serialize(record.getList(), target);
	}

	@Override
	public ListView<T> deserialize(DataInputView source) throws IOException {
		return new ListView<>(null, listSerializer.deserialize(source));
	}

	@Override
	public ListView<T> deserialize(ListView<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		listSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			canEqual(obj) && listSerializer.equals(((ListViewSerializer<?>) obj).listSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}

	@Override
	public int hashCode() {
		return listSerializer.hashCode();
	}

	@Override
	public ListViewSerializerSnapshot<T> snapshotConfiguration() {
		return new ListViewSerializerSnapshot<T>(this);
	}

	@Override
	public CompatibilityResult<ListView<T>> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {

		if (configSnapshot instanceof CollectionSerializerConfigSnapshot) {
			// backwards compatibility path;
			// Flink versions older or equal to 1.6.x returns a
			// CollectionSerializerConfigSnapshot as the snapshot
			Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>> previousElemSerializerAndConfig =
				((CollectionSerializerConfigSnapshot<?, ?>) configSnapshot).getSingleNestedSerializerAndConfig();

			// in older versions, the nested list serializer was always
			// specifically a ListSerializer, so this cast is safe
			ListSerializer<T> castedSer = (ListSerializer<T>) listSerializer;
			CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousElemSerializerAndConfig.f0,
				UnloadableDummyTypeSerializer.class,
				previousElemSerializerAndConfig.f1,
				castedSer.getElementSerializer());

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else {
				return CompatibilityResult.requiresMigration();
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
