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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Collector;

/**
 * Base class for a co-user-defined table function (CO-UDTF). A co-user-defined table functions
 * works on zero, one, or multiple scalar values as input from the connected table and returns
 * multiple rows as output.
 *
 * <p>The behavior of a {@link CoTableFunction} can be defined by implementing two custom evaluation
 * methods, i.e., named <code>eval1</code> and <code>eval2</code>. These two evaluation methods must
 * be declared publicly, not static. Evaluation methods can also be overloaded by implementing
 * multiple methods named <code>eval1</code> and <code>eval2</code>.
 *
 * <p>For Example:
 *
 * <pre>
 * {@code
 *   public class CoSplit extends CoTableFunction<String> {
 *
 *     // implement an "eval1" method with as many parameters as you want
 *     public void eval1(String str) {
 *       for (String s : str.split(" ")) {
 *         collect(s);   // use collect(...) to emit an output row
 *       }
 *     }
 *
 *     // implement an "eval2" method with as many parameters as you want
 *     public void eval2(String str) {
 *       for (String s : str.split(" ")) {
 *         collect(s);   // use collect(...) to emit an output row
 *       }
 *     }
 *     // you can overload the eval method here ...
 *   }
 *
 *   TableEnvironment tEnv = ...
 *   Table table1 = ...    // schema: ROW(a1 VARCHAR)
 *   Table table2 = ...    // schema: ROW(a2 VARCHAR)
 *
 *   // for Scala users
 *   val coSplit = new CoSplit()
 *   table1.connect(table2)
 *     .flatMap(coSplit('a1)('a2))
 * }
 * </pre>
 *
 * @param <T> The type of the output row
 */
@PublicEvolving
public abstract class CoTableFunction<T> extends UserDefinedFunction {

	// same with TableFunction
}
