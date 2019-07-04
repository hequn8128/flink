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

/**
 * Base class for a co-user-defined scalar function. A co-user-defined scalar functions maps zero,
 * one, or multiple scalar values from the connected table to a new scalar value.
 *
 * <p>The behavior of a {@link CoScalarFunction} can be defined by implementing two custom
 * evaluation methods, i.e, named <code>eval1</code> and <code>eval2</code>. These two evaluation
 * methods must be declared publicly. Evaluation methods can also be overloaded by implementing
 * multiple methods named <code>eval1</code> and <code>eval2</code>.
 */
@PublicEvolving
public abstract class CoScalarFunction extends UserDefinedFunction {

	// same with ScalarFunction
}
