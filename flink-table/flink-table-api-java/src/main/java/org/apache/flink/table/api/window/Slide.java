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
 * Helper class for creating a sliding window. Sliding windows have a fixed size and slide by
 * a specified slide interval. If the slide interval is smaller than the window size, sliding
 * windows are overlapping. Thus, an element can be assigned to multiple windows.
 *
 * <p>For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
 * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
 * consecutive window evaluations.
 *
 * <p>Java uses should usre the methods with parameters of String type, while Scala users should use
 * the methods with parameters of Expression type.
 *
 * <p>Example:
 *
 * <pre>
 * {@code
 *    Slide.over("10.minutes").every("5.minutes").on("rowtime").as("w")
 * }
 * </pre>
 */
public class Slide {

	/**
	 * Creates a sliding window. Sliding windows have a fixed size and slide by
	 * a specified slide interval. If the slide interval is smaller than the window size, sliding
	 * windows are overlapping. Thus, an element can be assigned to multiple windows.
	 *
	 * <p>For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
	 * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
	 * consecutive window evaluations.
	 *
	 * @param size the size of the window as time or row-count interval
	 * @return a partially specified sliding window
	 */
	public static SlideWithStringSize over(String size) {
		return new SlideWithStringSize(size);
	}

	/**
	 * Creates a sliding window. Sliding windows have a fixed size and slide by
	 * a specified slide interval. If the slide interval is smaller than the window size, sliding
	 * windows are overlapping. Thus, an element can be assigned to multiple windows.
	 *
	 * <p>For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
	 * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
	 * consecutive
	 *
	 * @param size the size of the window as time or row-count interval
	 * @return a partially specified sliding window
	 */
	public static SlideWithExpressionSize over(Expression size) {
		return new SlideWithExpressionSize(size);
	}

	/**
	 * Partially specified sliding window.
	 */
	public static class SlideWithStringSize {

		/** The size of the window either as time or row-count interval. */
		private final String size;

		public SlideWithStringSize(String size) {
			this.size = size;
		}

		/**
		 * Specifies the window's slide as time or row-count interval.
		 *
		 * <p>The slide determines the interval in which windows are started. Hence, sliding windows
		 * can overlap if the slide is smaller than the size of the window.
		 *
		 * <p>For example, you could have windows of size 15 minutes that slide by 3 minutes. With
		 * this 15 minutes worth of elements are grouped every 3 minutes and each row contributes
		 * to 5 windows.
		 *
		 * @param slide the slide of the window either as time or row-count interval.
		 * @return a sliding window
		 */
		public SlideWithStringSizeAndSlide every(String slide) {
			return new SlideWithStringSizeAndSlide(size, slide);
		}
	}

	/**
	 * Sliding window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class SlideWithStringSizeAndSlide {

		/** The size of the window either as time or row-count interval. */
		private final String size;
		private final String slide;

		public SlideWithStringSizeAndSlide(String size, String slide) {
			this.size = size;
			this.slide = slide;
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
		public SlideWithStringSizeAndSlideOnTime on(String timeField) {
			return new SlideWithStringSizeAndSlideOnTime(timeField, size, slide);
		}
	}

	/**
	 * Sliding window on time.
	 */
	public static class SlideWithStringSizeAndSlideOnTime {

		private final String timeField;
		private final String size;
		private final String slide;

		public SlideWithStringSizeAndSlideOnTime(String timeField, String size, String slide) {
			this.timeField = timeField;
			this.size = size;
			this.slide = slide;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and
		 * {@code select()} clause can refer to. {@code select()} statement can access window
		 * properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public SlideWithStringSizeAndSlideOnTimeWithAlias as(String alias) {
			return new SlideWithStringSizeAndSlideOnTimeWithAlias(alias, timeField, size, slide);
		}
	}

	/**
	 * Sliding window on time with alias. Fully specifies a window.
	 */
	public static class SlideWithStringSizeAndSlideOnTimeWithAlias implements GroupWindow {

		private final String alias;
		private final String timeField;
		private final String size;
		private final String slide;

		public SlideWithStringSizeAndSlideOnTimeWithAlias(
			String alias,
			String timeField,
			String size,
			String slide) {
			this.alias = alias;
			this.timeField = timeField;
			this.size = size;
			this.slide = slide;
		}

		public String getSize() {
			return size;
		}

		public String getTimeField() {
			return timeField;
		}

		public String getSlide() {
			return slide;
		}

		public String getAlias() {
			return alias;
		}
	}

	/**
	 * Partially specified sliding window.
	 */
	public static class SlideWithExpressionSize {

		/** The size of the window either as time or row-count interval. */
		private final Expression size;

		public SlideWithExpressionSize(Expression size) {
			this.size = size;
		}

		/**
		 * Specifies the window's slide as time or row-count interval.
		 *
		 * <p>The slide determines the interval in which windows are started. Hence, sliding windows
		 * can overlap if the slide is smaller than the size of the window.
		 *
		 * <p>For example, you could have windows of size 15 minutes that slide by 3 minutes. With
		 * this 15 minutes worth of elements are grouped every 3 minutes and each row contributes to
		 * 5 windows.
		 *
		 * @param slide the slide of the window either as time or row-count interval.
		 * @return a sliding window
		 */
		public SlideWithExpressionSizeAndSlide every(Expression slide) {
			return new SlideWithExpressionSizeAndSlide(size, slide);
		}
	}

	/**
	 * Sliding window.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 */
	public static class SlideWithExpressionSizeAndSlide {

		/** The size of the window either as time or row-count interval. */
		private final Expression size;
		private final Expression slide;

		public SlideWithExpressionSizeAndSlide(Expression size, Expression slide) {
			this.size = size;
			this.slide = slide;
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
		public SlideWithExpressionSizeAndSlideOnTime on(Expression timeField) {
			return new SlideWithExpressionSizeAndSlideOnTime(timeField, size, slide);
		}
	}

	/**
	 * Sliding window on time.
	 */
	public static class SlideWithExpressionSizeAndSlideOnTime {

		private final Expression timeField;
		private final Expression size;
		private final Expression slide;

		public SlideWithExpressionSizeAndSlideOnTime(
			Expression timeField,
			Expression size,
			Expression slide) {
			this.timeField = timeField;
			this.size = size;
			this.slide = slide;
		}

		/**
		 * Assigns an alias for this window that the following {@code groupBy()} and
		 * {@code select()} clause can refer to. {@code select()} statement can access window
		 * properties such as window start or end time.
		 *
		 * @param alias alias for this window
		 * @return this window
		 */
		public SlideWithExpressionSizeAndSlideOnTimeWithAlias as(Expression alias) {
			return new SlideWithExpressionSizeAndSlideOnTimeWithAlias(
				alias,
				timeField,
				size,
				slide);
		}
	}

	/**
	 * Sliding window on time with alias. Fully specifies a window.
	 */
	public static class SlideWithExpressionSizeAndSlideOnTimeWithAlias implements GroupWindow {

		private final Expression alias;
		private final Expression timeField;
		private final Expression size;
		private final Expression slide;

		public SlideWithExpressionSizeAndSlideOnTimeWithAlias(
			Expression alias,
			Expression timeField,
			Expression size,
			Expression slide) {
			this.alias = alias;
			this.timeField = timeField;
			this.size = size;
			this.slide = slide;
		}

		public Expression getSize() {
			return size;
		}

		public Expression getTimeField() {
			return timeField;
		}

		public Expression getSlide() {
			return slide;
		}

		public Expression getAlias() {
			return alias;
		}
	}

}
