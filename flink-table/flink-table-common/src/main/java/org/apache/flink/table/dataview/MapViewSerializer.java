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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerConfigSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.MapView;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A serializer for {@link MapView}. The serializer relies on a key serializer and a value
 * serializer for the serialization of the map's key-value pairs.
 *
 * <p>The serialization format for the map is as follows: four bytes for the length of the map,
 * followed by the serialized representation of each key-value pair. To allow null values,
 * each value is prefixed by a null marker.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
public class MapViewSerializer<K, V> extends TypeSerializer<MapView<K, V>> {

	private static final long serialVersionUID = -9007142882049098705L;

	/** The serializer for the elements of the map. */
	private final TypeSerializer<Map<K, V>> mapSerializer;

	public MapViewSerializer(TypeSerializer<Map<K, V>> mapSerializer) {
		this.mapSerializer = mapSerializer;
	}

	public TypeSerializer<Map<K, V>> getMapSerializer() {
		return mapSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<MapView<K, V>> duplicate() {
		return new MapViewSerializer<K, V>(mapSerializer.duplicate());
	}

	@Override
	public MapView<K, V> createInstance() {
		return new MapView<>();
	}

	@Override
	public MapView<K, V> copy(MapView<K, V> from) {
		return new MapView<>(null, null, mapSerializer.copy(from.getMap()));
	}

	@Override
	public MapView<K, V> copy(MapView<K, V> from, MapView<K, V> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		// var length
		return -1;
	}

	@Override
	public void serialize(MapView<K, V> record, DataOutputView target) throws IOException {
		mapSerializer.serialize(record.getMap(), target);
	}

	@Override
	public MapView<K, V> deserialize(DataInputView source) throws IOException {
		return new MapView<>(null, null, mapSerializer.deserialize(source));
	}

	@Override
	public MapView<K, V> deserialize(MapView<K, V> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		mapSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			canEqual(obj) && mapSerializer.equals(((MapViewSerializer<?, ?>) obj).mapSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}

	@Override
	public int hashCode() {
		return mapSerializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<MapView<K, V>> snapshotConfiguration() {
		return null;
	}

	// copy and modified from MapSerializer.ensureCompatibility
	@Override
	public CompatibilityResult<MapView<K, V>> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {

		if (configSnapshot instanceof MapSerializerConfigSnapshot) {
			// backwards compatibility path;
			// Flink versions older or equal to 1.5.x returns a
			// MapSerializerConfigSnapshot as the snapshot
			List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> previousKvSerializersAndConfigs =
				((MapSerializerConfigSnapshot<?, ?>) configSnapshot).getNestedSerializersAndConfigs();

			// in older versions, the nested map serializer was always
			// specifically a MapSerializer, so this cast is safe
			MapSerializer<K, V> castedSer = (MapSerializer<K, V>) mapSerializer;

			CompatibilityResult<K> keyCompatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousKvSerializersAndConfigs.get(0).f0,
				UnloadableDummyTypeSerializer.class,
				previousKvSerializersAndConfigs.get(0).f1,
				castedSer.getKeySerializer());

			CompatibilityResult<V> valueCompatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousKvSerializersAndConfigs.get(1).f0,
				UnloadableDummyTypeSerializer.class,
				previousKvSerializersAndConfigs.get(1).f1,
				castedSer.getValueSerializer());

			if (!keyCompatResult.isRequiresMigration() && !valueCompatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else {
				return CompatibilityResult.requiresMigration();
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
