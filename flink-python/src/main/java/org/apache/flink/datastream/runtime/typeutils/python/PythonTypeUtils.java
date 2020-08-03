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

package org.apache.flink.datastream.runtime.typeutils.python;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BigIntSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.InstantSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.datastream.typeinfo.python.PickledByteArrayTypeInfo;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.runtime.typeutils.serializers.python.BigDecSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DateSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A util class for converting the given TypeInformation to other objects.
 */
public class PythonTypeUtils {

	/**
	 * Get coder proto according to the given type information.
	 */
	public static class TypeInfoToProtoConverter {

		public static FlinkFnApi.TypeInfo.FieldType getFieldType(TypeInformation typeInformation) {

			if (typeInformation instanceof BasicTypeInfo) {
				return buildBasicTypeProto((BasicTypeInfo) typeInformation);
			}

			if (typeInformation instanceof PrimitiveArrayTypeInfo) {
				return buildPrimitiveArrayTypeProto((PrimitiveArrayTypeInfo) typeInformation);
			}

			if (typeInformation instanceof RowTypeInfo) {
				return buildRowTypeProto((RowTypeInfo) typeInformation);
			}

			if (typeInformation instanceof PickledByteArrayTypeInfo) {
				return buildPickledBytesTypeProto((PickledByteArrayTypeInfo) typeInformation);
			}

			throw new UnsupportedOperationException("The class of BasicTypeInfo is not supported");
		}

		public static FlinkFnApi.TypeInfo toTypeInfoProto(FlinkFnApi.TypeInfo.FieldType fieldType) {
			return FlinkFnApi.TypeInfo.newBuilder().addField(FlinkFnApi.TypeInfo.Field.newBuilder().setType(fieldType).build()).build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildBasicTypeProto(BasicTypeInfo basicTypeInfo) {

			FlinkFnApi.TypeInfo.TypeName typeName = null;

			if (basicTypeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BOOLEAN;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BYTE;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.STRING;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.SHORT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.INT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.INT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.LONG;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.FLOAT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.DOUBLE;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.CHAR;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.DATE_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.LOCAL_DATE;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.VOID_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.VOID;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BIG_INT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BIG_DEC;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.INSTANT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.INSTANT;
			}

			if (typeName == null) {
				throw new UnsupportedOperationException("The class of BasicTypeInfo is not supported");
			}

			return FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(typeName).build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildPrimitiveArrayTypeProto(
			PrimitiveArrayTypeInfo primitiveArrayTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType elementFieldType = null;
			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BOOLEAN_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BYTE_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.SHORT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.INT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.LONG_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.FLOAT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.DOUBLE_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.CHAR_TYPE_INFO);
			}

			if (elementFieldType == null) {
				throw new UnsupportedOperationException("The class of BasicTypeInfo is not supported");
			}

			FlinkFnApi.TypeInfo.FieldType.Builder builder = FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.TypeInfo.TypeName.ARRAY);
			builder.setCollectionElementType(elementFieldType);
			return builder.build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildRowTypeProto(RowTypeInfo rowTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType.Builder builder =
				FlinkFnApi.TypeInfo.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.TypeInfo.TypeName.ROW);

			FlinkFnApi.TypeInfo.Builder rowTypeInfoBuilder = FlinkFnApi.TypeInfo.newBuilder();

			int arity = rowTypeInfo.getArity();
			for (int index = 0; index < arity; index++) {
				rowTypeInfoBuilder.addField(
					FlinkFnApi.TypeInfo.Field.newBuilder()
						.setName(rowTypeInfo.getFieldNames()[index])
						.setType(TypeInfoToProtoConverter.getFieldType(rowTypeInfo.getTypeAt(index)))
						.build());
			}
			builder.setRowTypeInfo(rowTypeInfoBuilder.build());
			return builder.build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildPickledBytesTypeProto(PickledByteArrayTypeInfo pickledByteArrayTypeInfo) {
			return FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.TypeInfo.TypeName.PICKLED_BYTES).build();
		}

	}

	/**
	 * Get serializers according to the given typeInformation.
	 */
	public static class TypeInfoToSerializerConverter {
		private static final Map<Class, TypeSerializer> typeInfoToSerialzerMap = new HashMap<>();

		static {
			typeInfoToSerialzerMap.put(BasicTypeInfo.BOOLEAN_TYPE_INFO.getTypeClass(), BooleanSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.INT_TYPE_INFO.getTypeClass(), IntSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.STRING_TYPE_INFO.getTypeClass(), StringSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.SHORT_TYPE_INFO.getTypeClass(), ShortSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.LONG_TYPE_INFO.getTypeClass(), LongSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.FLOAT_TYPE_INFO.getTypeClass(), FloatSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.DOUBLE_TYPE_INFO.getTypeClass(), DoubleSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.CHAR_TYPE_INFO.getTypeClass(), CharSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.DATE_TYPE_INFO.getTypeClass(), DateSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.VOID_TYPE_INFO.getTypeClass(), VoidSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.BIG_INT_TYPE_INFO.getTypeClass(), BigIntSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.BIG_DEC_TYPE_INFO.getTypeClass(), BigDecSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.INSTANT_TYPE_INFO.getTypeClass(), InstantSerializer.INSTANCE);
		}

		public static TypeSerializer typeInfoSerializerConverter(TypeInformation typeInformation) {
			TypeSerializer typeSerializer = typeInfoToSerialzerMap.get(typeInformation.getTypeClass());
			if (typeSerializer != null) {
				return typeSerializer;
			} else {

				if (typeInformation instanceof PickledByteArrayTypeInfo) {
					return BytePrimitiveArraySerializer.INSTANCE;
				}

				if (typeInformation instanceof RowTypeInfo) {
					RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInformation;
					TypeSerializer[] fieldTypeSerializers = Arrays.stream(rowTypeInfo.getFieldTypes())
						.map(f -> typeInfoSerializerConverter(f)).toArray(TypeSerializer[]::new);
					return new RowSerializer(fieldTypeSerializers);
				}
			}

			throw new UnsupportedOperationException(
				String.format("Could not find type serializer for current type [%s].", typeInformation.toString()));
		}
	}

}
