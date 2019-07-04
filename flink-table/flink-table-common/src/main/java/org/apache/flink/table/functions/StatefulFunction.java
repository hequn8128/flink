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
 * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
 * or multiple scalar values to a new scalar value.
 *
 * <p>The behavior of a {@link StatefulFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly and named <code>eval</code>. Evaluation
 * methods can also be overloaded by implementing multiple methods named <code>eval</code>.
 *
 * <p>User-defined functions must have a default constructor and must be instantiable during runtime.
 *
 * <p>By default the result type of an evaluation method is determined by Flink's type extraction
 * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
 * complex, custom, or composite types. In these cases {@link TypeInformation} of the result type
 * can be manually defined by overriding {@link StatefulFunction#getStateType}.
 *
 * <p>Internally, the Table/SQL API code generation works with primitive values as much as possible.
 * If a user-defined scalar function should not introduce much overhead during runtime, it is
 * recommended to declare parameters and result types as primitive types instead of their boxed
 * classes. <code>DATE/TIME</code> is equal to <code>int</code>, <code>TIMESTAMP</code> is equal
 * to <code>long</code>.
 */
@PublicEvolving
public interface StatefulFunction<STATE> {

	/**
	 * This method is called when the parallel function instance is created during distributed
	 * execution. Functions typically set up their state storing data structures in this method.
	 *
	 * @param state the state which has been initialized by the system.
	 * @throws Exception
	 */
	public abstract void initializeWithState(STATE state) throws Exception;

	/**
	 * This method is called when a snapshot for a checkpoint is requested.
	 *
	 * @param state the state which has been initialized by the system.
	 * @throws Exception
	 */
	public default void snapshotState(STATE state) throws Exception {}

	/**
	 * Returns the {@link TypeInformation} of the {@link StatefulFunction}'s state.
	 *
	 * @return The {@link TypeInformation} of the {@link StatefulFunction}'s state
	 *         or <code>null</code> if the state type should be automatically inferred.
	 */
	default TypeInformation<STATE> getStateType() {
		return null;
	}
}
