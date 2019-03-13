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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A group window specification.
 *
 * <p>Group windows group rows based on time or row-count intervals and is therefore essentially a
 * special type of groupBy. Just like groupBy, group windows allow to compute aggregates
 * on groups of elements.
 *
 * <p>Infinite streaming tables can only be grouped into time or row intervals. Hence window
 * grouping is required to apply aggregations on streaming tables.
 *
 * <p>For finite batch tables, group windows provide shortcuts for time-based groupBy.
 *
 * <p>Note: {@link Window} is temporally used as the father class of {@link GroupWindow} for the
 * sake of compatibility. It will be removed later.
 */
@PublicEvolving
public interface GroupWindow extends Window {

}
