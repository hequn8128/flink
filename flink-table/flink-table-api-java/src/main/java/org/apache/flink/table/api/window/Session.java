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
 * Helper class for creating a session window. The boundary of session windows are defined by
 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
 * gap period.
 *
 * <p>Java users should use the methods with parameters of String type, while Scala users should use
 * the methods with parameters of Expression type.
 *
 * <p>Example:
 *
 * <pre>
 * {@code
 *    Session.withGap("10.minutes").on("rowtime").as("w")
 * }
 * </pre>
 */
public class Session {

	/**
	 * Creates a session window. The boundary of session windows are defined by
	 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
	 * gap period.
	 *
	 * @param gap specifies how long (as interval of milliseconds) to wait for new data before
	 *            closing the session window.
	 * @return a partially defined session window
	 */
	public static SessionWithStringGap withGap(String gap) {
		return new SessionWithStringGap(gap);
	}

	/**
	 * Creates a session window. The boundary of session windows are defined by
	 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
	 * gap period.
	 *
	 * @param gap specifies how long (as interval of milliseconds) to wait for new data before
	 *            closing the session window.
	 * @return a partially defined session window
	 */
	public static SessionWithExpressionGap withGap(Expression gap) {
		return new SessionWithExpressionGap(gap);
	}

	/**
	 * Session window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class SessionWithStringGap {

		/** the time interval of inactivity before a window is closed. */
		private final String gap;

		public SessionWithStringGap(String gap) {
			this.gap = gap;
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
		public SessionWithStringGapOnTime on(String timeField) {
			return new SessionWithStringGapOnTime(timeField, gap);
		}
	}

	/**
	 * Session window on time.
	 */
	public static class SessionWithStringGapOnTime {

		private final String timeField;
		private final String gap;

		public SessionWithStringGapOnTime(String timeField, String gap) {
			this.timeField = timeField;
			this.gap = gap;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and
		 * {@code select()} clause can refer to. {@code select()} statement can access window
		 * properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public SessionWithStringGapOnTimeWithAlias as(String alias) {
			return new SessionWithStringGapOnTimeWithAlias(alias, timeField, gap);
		}
	}

	/**
	 * Session window on time with alias. Fully specifies a window.
	 */
	public static class SessionWithStringGapOnTimeWithAlias implements GroupWindow {

		private final String alias;
		private final String timeField;
		private final String gap;

		public SessionWithStringGapOnTimeWithAlias(String alias, String timeField, String gap) {
			this.alias = alias;
			this.timeField = timeField;
			this.gap = gap;
		}

		public String getGap() {
			return gap;
		}

		public String getTimeField() {
			return timeField;
		}

		public String getAlias() {
			return alias;
		}
	}

	/**
	 * Session window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class SessionWithExpressionGap {

		/** the time interval of inactivity before a window is closed. */
		private final Expression gap;

		public SessionWithExpressionGap(Expression gap) {
			this.gap = gap;
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
		public SessionWithExpressionGapOnTime on(Expression timeField) {
			return new SessionWithExpressionGapOnTime(timeField, gap);
		}
	}

	/**
	 * Session window on time.
	 */
	public static class SessionWithExpressionGapOnTime {

		private final Expression timeField;
		private final Expression gap;

		public SessionWithExpressionGapOnTime(Expression timeField, Expression gap) {
			this.timeField = timeField;
			this.gap = gap;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and
		 * {@code select()} clause can refer to. {@code select()} statement can access window
		 * properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public SessionWithExpressionGapOnTimeWithAlias as(Expression alias) {
			return new SessionWithExpressionGapOnTimeWithAlias(alias, timeField, gap);
		}
	}

	/**
	 * Session window on time with alias. Fully specifies a window.
	 */
	public static class SessionWithExpressionGapOnTimeWithAlias implements GroupWindow {

		private final Expression alias;
		private final Expression timeField;
		private final Expression gap;

		public SessionWithExpressionGapOnTimeWithAlias(
			Expression alias,
			Expression timeField,
			Expression gap) {
			this.alias = alias;
			this.timeField = timeField;
			this.gap = gap;
		}

		public Expression getGap() {
			return gap;
		}

		public Expression getTimeField() {
			return timeField;
		}

		public Expression getAlias() {
			return alias;
		}
	}
}
