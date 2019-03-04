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

import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FunctionDefinitions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helper object for creating an over window.
 *
 * <p>Java users should use the methods with parameters of String type, while Scala users should use
 * the methods with parameters of Expression type.
 *
 * <p>Example:
 *
 * <pre>
 * {@code
 *    Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_range").as("w")
 * }
 * </pre>
 */
public class Over {

	/**
	 * Specifies the time attribute on which rows are grouped.
	 *
	 * <p>For streaming tables call {@code orderBy("rowtime")} or {@code orderBy("proctime")} to
	 * specify time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 */
	public static OverWithStringOrderBy orderBy(String size) {
		return new OverWithStringOrderBy("", size);
	}

	/**
	 * Specifies the time attribute on which rows are grouped.
	 *
	 * <p>For streaming tables call {@code orderBy 'rowtime} or {@code orderBy 'proctime} to
	 * specify time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 */
	public static OverWithExpressionOrderBy orderBy(Expression size) {
		return new OverWithExpressionOrderBy(new ArrayList<>(), size);
	}

	/**
	 * Partitions the elements on some partition keys.
	 *
	 * @param partitionBy some partition keys.
	 * @return A partitionedOver instance that only contains the orderBy method.
	 */
	public static OverWithStringPartitionBy partitionBy(String partitionBy) {
		return new OverWithStringPartitionBy(partitionBy);
	}

	/**
	 * Partitions the elements on some partition keys.
	 *
	 * @param partitionBy some partition keys.
	 * @return A partitionedOver instance that only contains the orderBy method.
	 */
	public static OverWithExpressionPartitionBy partitionBy(Expression... partitionBy) {
		return new OverWithExpressionPartitionBy(Arrays.asList(partitionBy));
	}

	/**
	 * Over window which was generated after performing partitionBy.
	 */
	public static class OverWithStringPartitionBy {

		private final String partitionBy;

		public OverWithStringPartitionBy(String partitionBy) {
			this.partitionBy = partitionBy;
		}

		/**
		 * Specifies the time attribute on which rows are grouped.
		 *
		 * <p>For streaming tables call {@code orderBy 'rowtime} or {@code orderBy 'proctime} to
		 * specify time mode.
		 *
		 * <p>For batch tables, refer to a timestamp or long attribute.
		 */
		public OverWithStringOrderBy orderBy(String orderBy) {
			return new OverWithStringOrderBy(partitionBy, orderBy);
		}
	}

	/**
	 * Over window which was generated after performing order by.
	 */
	public static class OverWithStringOrderBy {

		private final String partitionBy;
		private final String orderBy;

		public OverWithStringOrderBy(String partitionBy, String orderBy) {
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
		}

		/**
		 * Set the preceding offset (based on time or row-count intervals) for over window.
		 *
		 * @param preceding preceding offset relative to the current row.
		 * @return this over window
		 */
		public OverWithStringPreceding preceding(String preceding) {
			return new OverWithStringPreceding(partitionBy, orderBy, preceding);
		}

		/**
		 * Assigns an alias for this window that the following {@code select()} clause can refer to.
		 *
		 * @param alias alias for this over window
		 * @return over window
		 */
		public OverWithString as(String alias) {
			return new OverWithString(
				alias,
				partitionBy,
				orderBy,
				"unbounded_range",
				"current_range");
		}
	}

	/**
	 * A partially defined over window.
	 */
	public static class OverWithStringPreceding {

		private final String partitionBy;
		private final String orderBy;
		private final String preceding;

		private String following = null;

		public OverWithStringPreceding(String partitionBy, String orderBy, String preceding) {
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
			this.preceding = preceding;
		}

		/**
		 * Assigns an alias for this window that the following {@code select()} clause can refer to.
		 *
		 * @param alias alias for this over window
		 * @return over window
		 */
		public OverWithString as(String alias) {
			return new OverWithString(alias, partitionBy, orderBy, preceding, following);
		}

		/**
		 * Set the following offset (based on time or row-count intervals) for over window.
		 *
		 * @param following following offset that relative to the current row.
		 * @return this over window
		 */
		public OverWithStringPreceding following(String following) {
			this.following = following;
			return this;
		}
	}

	/**
	 * Over window is similar to the traditional OVER SQL.
	 */
	public static class OverWithString implements OverWindow {

		private final String alias;
		private final String partitionBy;
		private final String orderBy;
		private final String preceding;
		private final String following;

		public OverWithString(
			String alias,
			String partitionBy,
			String orderBy,
			String preceding,
			String following) {
			this.alias = alias;
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
			this.preceding = preceding;
			this.following = following;
		}

		public String getPartitionBy() {
			return partitionBy;
		}

		public String getOrderBy() {
			return orderBy;
		}

		public String getPreceding() {
			return preceding;
		}

		public String getAlias() {
			return alias;
		}

		public String getFollowing() {
			return following;
		}
	}

	/**
	 * Over window which was generated after performing partitionBy.
	 */
	public static class OverWithExpressionPartitionBy {

		private final List<Expression> partitionBy;

		public OverWithExpressionPartitionBy(List<Expression> partitionBy) {
			this.partitionBy = partitionBy;
		}

		/**
		 * Specifies the time attribute on which rows are grouped.
		 *
		 * <p>For streaming tables call {@code orderBy 'rowtime} or {@code orderBy 'proctime} to
		 * specify time mode.
		 *
		 * <p>For batch tables, refer to a timestamp or long attribute.
		 */
		public OverWithExpressionOrderBy orderBy(Expression orderBy) {
			return new OverWithExpressionOrderBy(partitionBy, orderBy);
		}
	}

	/**
	 * Over window which was generated after performing order by.
	 */
	public static class OverWithExpressionOrderBy {

		private final List<Expression> partitionBy;
		private final Expression orderBy;

		public OverWithExpressionOrderBy(List<Expression> partitionBy, Expression orderBy) {
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
		}

		/**
		 * Set the preceding offset (based on time or row-count intervals) for over window.
		 *
		 * @param preceding preceding offset relative to the current row.
		 * @return this over window
		 */
		public OverWithExpressionPreceding preceding(Expression preceding) {
			return new OverWithExpressionPreceding(partitionBy, orderBy, preceding);
		}

		/**
		 * Assigns an alias for this window that the following {@code select()} clause can refer to.
		 *
		 * @param alias alias for this over window
		 * @return over window
		 */
		public OverWithExpression as(Expression alias) {
			Expression unbounedRange =
				new CallExpression(FunctionDefinitions.UNBOUNDED_RANGE, new ArrayList<>());
			Expression currentRange =
				new CallExpression(FunctionDefinitions.CURRENT_RANGE, new ArrayList<>());
			return new OverWithExpression(alias, partitionBy, orderBy, unbounedRange, currentRange);
		}
	}

	/**
	 * A partially defined over window.
	 */
	public static class OverWithExpressionPreceding {

		private final List<Expression> partitionBy;
		private final Expression orderBy;
		private final Expression preceding;

		private Expression following = null;

		public OverWithExpressionPreceding(
			List<Expression> partitionBy,
			Expression orderBy,
			Expression preceding) {
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
			this.preceding = preceding;
		}

		/**
		 * Assigns an alias for this window that the following {@code select()} clause can refer to.
		 *
		 * @param alias alias for this over window
		 * @return over window
		 */
		public OverWithExpression as(Expression alias) {
			return new OverWithExpression(alias, partitionBy, orderBy, preceding, following);
		}

		/**
		 * Set the following offset (based on time or row-count intervals) for over window.
		 *
		 * @param following following offset that relative to the current row.
		 * @return this over window
		 */
		public OverWithExpressionPreceding following(Expression following) {
			this.following = following;
			return this;
		}
	}

	/**
	 * Over window is similar to the traditional OVER SQL.
	 */
	public static class OverWithExpression implements OverWindow {

		private final Expression alias;
		private final List<Expression> partitionBy;
		private final Expression orderBy;
		private final Expression preceding;
		private final Expression following;

		public OverWithExpression(
			Expression alias,
			List<Expression> partitionBy,
			Expression orderBy,
			Expression preceding,
			Expression following) {
			this.alias = alias;
			this.partitionBy = partitionBy;
			this.orderBy = orderBy;
			this.preceding = preceding;
			this.following = following;
		}

		public Expression getAlias() {
			return alias;
		}

		public List<Expression> getPartitionBy() {
			return partitionBy;
		}

		public Expression getOrderBy() {
			return orderBy;
		}

		public Expression getPreceding() {
			return preceding;
		}

		public Expression getFollowing() {
			return following;
		}
	}
}
