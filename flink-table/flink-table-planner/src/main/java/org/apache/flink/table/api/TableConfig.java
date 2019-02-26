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

import java.math.MathContext;
import java.util.TimeZone;

/**
 * A config to define the runtime behavior of the Table API.
 */
public class TableConfig {

	/**
	 * Defines the timezone for date/time/timestamp conversions.
	 */
	private TimeZone timeZone = TimeZone.getTimeZone("UTC");
	/**
	 * Defines if all fields need to be checked for NULL first.
	 */
	private Boolean nullCheck = true;
	/**
	 * Defines the default context for decimal division calculation.
	 * We use Scala's default MathContext.DECIMAL128.
	 */
	private MathContext decimalContext = MathContext.DECIMAL128;
	/**
	 * Specifies a threshold where generated code will be split into sub-function calls. Java has a
	 * maximum method length of 64 KB. This setting allows for finer granularity if necessary.
	 */
	private Integer maxGeneratedCodeLength = 64000; // just an estimate

	public static final TableConfig DEFAULT = new TableConfig();

	/**
	 * Returns the timezone for date/time/timestamp conversions.
	 */
	public TimeZone getTimeZone() {
		return timeZone;
	}

	/**
	 * Sets the timezone for date/time/timestamp conversions.
	 */
	public void setTimeZone(TimeZone timeZone) {
		this.timeZone = timeZone;
	}

	/**
	 * Returns the NULL check. If enabled, all fields need to be checked for NULL first.
	 */
	public Boolean getNullCheck() {
		return nullCheck;
	}

	/**
	 * Sets the NULL check. If enabled, all fields need to be checked for NULL first.
	 */
	public void setNullCheck(Boolean nullCheck) {
		this.nullCheck = nullCheck;
	}

	/**
	 * Returns the default context for decimal division calculation.
	 * [[_root_.java.math.MathContext#DECIMAL128]] by default.
	 */
	public MathContext getDecimalContext() {
		return decimalContext;
	}

	/**
	 * Sets the default context for decimal division calculation.
	 * [[_root_.java.math.MathContext#DECIMAL128]] by default.
	 */
	public void setDecimalContext(MathContext decimalContext) {
		this.decimalContext = decimalContext;
	}

	/**
	 * Returns the current threshold where generated code will be split into sub-function calls.
	 * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
	 * necessary. Default is 64000.
	 */
	public Integer getMaxGeneratedCodeLength() {
		return maxGeneratedCodeLength;
	}

	/**
	 * Returns the current threshold where generated code will be split into sub-function calls.
	 * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
	 * necessary. Default is 64000.
	 */
	public void setMaxGeneratedCodeLength(Integer maxGeneratedCodeLength) {
		this.maxGeneratedCodeLength = maxGeneratedCodeLength;
	}
}
