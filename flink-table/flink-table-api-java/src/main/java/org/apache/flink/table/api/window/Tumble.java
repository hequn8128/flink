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

package org.apache.flink.table.api.window;

import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.expressions.Expression;

/**
 * Helper object for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
 * elements in 5 minutes intervals.
 *
 * <p>Java users should use the methods with parameters of String type, while Scala users should use
 * the methods with parameters of Expression type.
 *
 * <p>Example:
 *
 * <pre>
 * {@code
 *    Tumble.over("10.minutes").on("rowtime").as("w")
 * }
 * </pre>
 */
public class Tumble {

	/**
	 * Creates a tumbling window. Tumbling windows are consecutive, non-overlapping
	 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 */
	public static TumbleWithStringSize over(String size) {
		return new TumbleWithStringSize(size);
	}

	/**
	 * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
	 * windows. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 */
	public static TumbleWithExpressionSize over(Expression size) {
		return new TumbleWithExpressionSize(size);
	}

	/**
	 * Tumbling window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class TumbleWithStringSize {

		/** The size of the window either as time or row-count interval. */
		private final String size;

		public TumbleWithStringSize(String size) {
			this.size = size;
		}

		/**
		 * Specifies the time attribute on which rows are grouped.
		 *
		 * <p>For streaming tables you can specify grouping by a event-time or processing-time
		 * attribute.
		 *
		 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
		 *
		 * @param timeField time attribute for streaming and batch tables
		 * @return a tumbling window on event-time
		 */
		public TumbleWithStringSizeOnTime on(String timeField) {
			return new TumbleWithStringSizeOnTime(timeField, size);
		}
	}

	/**
	 * Tumbling window on time.
	 */
	public static class TumbleWithStringSizeOnTime {

		private final String time;
		private final String size;

		public TumbleWithStringSizeOnTime(String time, String size) {
			this.time = time;
			this.size = size;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and {@code select()} clause can
		 * refer to. {@code select()} statement can access window properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public TumbleWithStringSizeOnTimeWithAlias as(String alias) {
			return new TumbleWithStringSizeOnTimeWithAlias(alias, time, size);
		}
	}

	/**
	 * Tumbling window on time with alias. Fully specifies a window.
	 */
	public static class TumbleWithStringSizeOnTimeWithAlias implements GroupWindow {

		private final String size;
		private final String timeField;
		private final String alias;

		public TumbleWithStringSizeOnTimeWithAlias(String alias, String timeField, String size) {
			this.alias = alias;
			this.timeField = timeField;
			this.size = size;
		}

		public String getSize() {
			return size;
		}

		public String getTimeField() {
			return timeField;
		}

		public String getAlias() {
			return alias;
		}
	}

	/**
	 * Tumbling window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class TumbleWithExpressionSize {

		/** The size of the window either as time or row-count interval. */
		private final Expression size;

		public TumbleWithExpressionSize(Expression size) {
			this.size = size;
		}

		/**
		 * Specifies the time attribute on which rows are grouped.
		 *
		 * <p>For streaming tables you can specify grouping by a event-time or processing-time
		 * attribute.
		 *
		 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
		 *
		 * @param timeField time attribute for streaming and batch tables
		 * @return a tumbling window on event-time
		 */
		public TumbleWithExpressionSizeOnTime on(Expression timeField) {
			return new TumbleWithExpressionSizeOnTime(timeField, size);
		}
	}

	/**
	 * Tumbling window on time.
	 */
	public static class TumbleWithExpressionSizeOnTime {

		private final Expression time;
		private final Expression size;

		public TumbleWithExpressionSizeOnTime(Expression time, Expression size) {
			this.time = time;
			this.size = size;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and
		 * {@code select()} clause can refer to. {@code select()} statement can access window
		 * properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public TumbleWithExpressionSizeOnTimeWithAlias as(Expression alias) {
			return new TumbleWithExpressionSizeOnTimeWithAlias(alias, time, size);
		}
	}

	/**
	 * Tumbling window on time with alias. Fully specifies a window.
	 */
	public static class TumbleWithExpressionSizeOnTimeWithAlias implements GroupWindow {

		private final Expression alias;
		private final Expression timeField;
		private final Expression size;

		public TumbleWithExpressionSizeOnTimeWithAlias(Expression alias, Expression timeField, Expression size) {
			this.alias = alias;
			this.timeField = timeField;
			this.size = size;
		}

		public Expression getSize() {
			return size;
		}

		public Expression getTimeField() {
			return timeField;
		}

		public Expression getAlias() {
			return alias;
		}
	}
}
