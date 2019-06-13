package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


public class WordCountDistinctTableAPI {

	public static class SplitTableFunction extends TableFunction<String> {
		public void eval(String line) {
			String[] words = line.split(" ");
			for (int i = 0; i < words.length; ++i) {
				collect(words[i]);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		DataStream ds = env.socketTextStream("localhost", 9000);
		tableEnv.registerDataStream("sourceTable", ds, "line, proctime.proctime");

		SplitTableFunction splitFunc = new SplitTableFunction();
		tableEnv.registerFunction("splitFunc", splitFunc);
		Table result = tableEnv.scan("sourceTable")
				.joinLateral("splitFunc(line) as word")
				.window(Tumble.over("5.seconds").on("proctime").as("w"))
				.groupBy("w")
				.select("count.distinct(word)");

		tableEnv.toAppendStream(result, Row.class).print();
		env.execute();
	}
}
