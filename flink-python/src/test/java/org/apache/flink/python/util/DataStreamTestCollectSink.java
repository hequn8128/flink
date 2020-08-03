/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A SinkFunction for collecting results of DataStream transformations in test cases.
 */
public class DataStreamTestCollectSink<IN> implements SinkFunction<IN> {

	public static List<Object> collectedResult = new ArrayList<>();
	private boolean isPythonObjects = false;

	public DataStreamTestCollectSink(boolean isPythonObjects) {
		this.isPythonObjects = isPythonObjects;
	}

	/**
	 * Collect the sink value into a static List so that the client side can fetch the result of flink job in test
	 * cases. if the output value is a byte array generated by pickle, it will be added to the list directly letting the
	 * client to deserialize the pickled bytes to python objects. Otherwise, the value will be added to the list in
	 * string format.
	 */
	@Override
	public void invoke(IN value, Context context) throws Exception {

		synchronized (collectedResult){
			collectedResult.add(isPythonObjects ? value : value.toString());
		}
	}

	public List<Object> collectAndClear() {
		List<Object> listToBeReturned = new ArrayList<>(collectedResult);
		collectedResult.clear();
		return listToBeReturned;
	}
}
