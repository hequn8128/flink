package org.apache.flink.ml.pipeline;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.TypeStringUtils;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * LocalPredictable get a {@link LocalPredictor} using {@link TableSchema}
 * or string representation of {@link TableSchema}.
 */
public interface LocalPredictable {

	LocalPredictor getLocalPredictor(TableSchema inputSchema) throws Exception;

	default LocalPredictor getLocalPredictor(String inputSchemaStr) throws Exception {
		return getLocalPredictor(schemaStr2Schema(inputSchemaStr));
	}

	static TableSchema schemaStr2Schema(String schemaStr) {
		String[] fields = schemaStr.split(",");
		String[] colNames = new String[fields.length];
		TypeInformation[] colTypes = new TypeInformation[fields.length];
		for (int i = 0; i < colNames.length; i++) {
			String[] kv = fields[i].trim().split("\\s+");
			colNames[i] = kv[0];
			if (colNames[i].equalsIgnoreCase("varbinary")) {
				colTypes[i] = BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			} else {
				colTypes[i] = TypeStringUtils.readTypeInfo(kv[1].toUpperCase());
			}
		}
		return new TableSchema(colNames, colTypes);
	}
}
