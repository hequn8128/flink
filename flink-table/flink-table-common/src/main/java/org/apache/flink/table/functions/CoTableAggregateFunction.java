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
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

/**
 * Base class for co-user-defined table aggregates.
 *
 * <p>The behavior of a {@link CoTableAggregateFunction} can be defined by implementing a series of
 * custom methods. A {@link CoTableAggregateFunction} needs at least four methods:
 * <ul>
 *     <li>createAccumulator</li>
 *     <li>accumulate1</li>
 *     <li>accumulate2</li>
 *     <li>emitValue or emitUpdateWithRetract or emitUpdateWithoutRetract</li>
 * </ul>
 *
 * <p>There are another methods that can be optional to have:
 * <ul>
 *     <li>retract1</li>
 *     <li>retract2</li>
 * </ul>
 *
 * <p>All these methods must be declared publicly, not static, and named exactly as the names
 * mentioned above. The method {@link #createAccumulator()} is defined in
 * the {@link UserDefinedAggregateFunction} functions, while other methods are explained below.
 *
 * <pre>
 * {@code
 * Processes the left input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
 * requires at least one accumulate() method.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulate1(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Processes the right input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
 * requires at least one accumulate() method.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulate2(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Retracts the left input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retract1(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Retracts the right input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retract2(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrive) or the final
 * result of the aggregation.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the collector used to output data.
 *
 * public void emitValue(ACC accumulator, Collector<T> out)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrive) or the final
 * result of the aggregation.
 *
 * Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated.
 * This method outputs data incrementally in retract mode, i.e., once there is an update, we have
 * to retract old records before sending new updated ones. The emitUpdateWithRetract method will be
 * used in preference to the emitValue method if both methods are defined in the table aggregate
 * function, because the method is treated to be more efficient than emitValue as it can output
 * values incrementally.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the retractable collector used to output data. Use collect method
 *                              to output(add) records and use retract method to retract(delete)
 *                              records.
 *
 * public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrive) or the final
 * result of the aggregation.
 *
 * Similar to emitUpdateWithRetract, emitUpdateWithoutRetract is used to emit values that have been
 * updated. However, this method outputs data incrementally in upsert mode, i.e., once there is an
 * update, we can only send the new updated records without retracting old ones.
 * The emitUpdateWithoutRetract method will be used in preference to the emitValue method if both
 * methods are defined in the table aggregate function, because the method is treated to be more
 * efficient than emitValue as it can output values incrementally.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the retractable collector used to output data. Use collect method
 *                              to output(add) records and use retract method to retract(delete)
 *                              records.
 *
 * public void emitUpdateWithoutRetract(ACC accumulator, RetractableCollector<T> out)
 * }
 * </pre>
 *
 * <p>We notice that there are three kinds of emit methods: emitValue, emitUpdateWithRetract and
 * emitUpdateWithoutRetract. Whether use emitUpdateWithoutRetract or emitUpdateWithRetract will be
 * decided by the query optimizer and these two methods will be used in preference to the emitValue
 * method if they are all defined in the table aggregate function, because they are treated to be
 * more efficient than emitValue as they can output values incrementally.
 * </p>
 *
 * @param <T>   the type of the table aggregation result
 * @param <ACC> the type of the table aggregation accumulator. The accumulator is used to keep the
 *              aggregated values which are needed to compute an aggregation result.
 *              TableAggregateFunction represents its state using accumulator, thereby the state of
 *              the TableAggregateFunction must be put into the accumulator.
 */
@PublicEvolving
public abstract class CoTableAggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {

	// same with TableAggregateFunction
}
