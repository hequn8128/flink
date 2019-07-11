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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.StatefulFunction;

import java.util.Arrays;

/**
 * Test scalar functions.
 */
public class JavaUserDefinedScalarFunctions {

	public static class MyState {
		public ListView<String> listView;
	}
	/**
	 * Increment input.
	 */
	public static class JavaFunc0 extends ScalarFunction implements StatefulFunction<MyState> {
		public long eval(Long l) {
			return l + 1;
		}

		public long retract(Long l) {
			return l + 1;
		}

		@Override
		public void initializeWithState(MyState myState) throws Exception {

		}

		@Override
		public void snapshotState(MyState myState) throws Exception {

		}

		@Override
		public TypeInformation<MyState> getStateType() {
			return null;
		}

		@Override
		public boolean eligibleForRedistribution() {
			return true;
		}
	}

	/**
	 * Concatenate inputs as strings.
	 */
	public static class JavaFunc1 extends ScalarFunction {
		public String eval(Integer a, int b,  Long c) {
			return a + " and " + b + " and " + c;
		}
	}

	/**
	 * Append product to string.
	 */
	public static class JavaFunc2 extends ScalarFunction {
		public String eval(String s, Integer... a) {
			int m = 1;
			for (int n : a) {
				m *= n;
			}
			return s + m;
		}
	}

	/**
	 * Test overloading.
	 */
	public static class JavaFunc3 extends ScalarFunction {
		public int eval(String a, int... b) {
			return b.length;
		}

		public String eval(String c) {
			return c;
		}
	}

	/**
	 * Concatenate arrays as strings.
	 */
	public static class JavaFunc4 extends ScalarFunction {
		public String eval(Integer[] a, String[] b) {
			return Arrays.toString(a) + " and " + Arrays.toString(b);
		}
	}

}
