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
import org.apache.flink.table.api.window.Over;

/**
 * Base class for Over Window.
 *
 * <p>Over window is similar to the traditional OVER SQL.
 *
 * <p>The OverWindow defines a range of rows over which aggregates are computed. OverWindow is not
 * an interface that users can implement. Instead, the Table API provides the {@link Over} class to
 * configure the properties of the over window. Over windows can be defined on event-time or
 * processing-time and on ranges specified as time interval or row-count.
 */
@PublicEvolving
public interface OverWindow {

}
