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

package org.apache.flink.python.metric;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to Flink
 * accumulators and metrics.
 *
 * <p>Using accumulators can be turned off because it is memory and network intensive. The
 * accumulator results are only meaningful in batch applications or testing streaming applications
 * which have a defined end. They are not essential during execution because metrics will also be
 * reported using the configured metrics reporter.
 *
 * <p>Note: This file is based on the FlinkMetricContainer class of Apache Beam.
 */
public class FlinkMetricContainer {

	private static final String METRIC_KEY_SEPARATOR =
		GlobalConfiguration.loadConfiguration().getString(MetricOptions.SCOPE_DELIMITER);

	private final MetricsContainerStepMap metricsContainers;
	private final MetricGroup baseMetricGroup;
	private final Map<String, Counter> flinkCounterCache;
	private final Map<String, Meter> flinkMeterCache;
	private final Map<String, FlinkDistributionGauge> flinkDistributionGaugeCache;
	private final Map<String, FlinkGauge> flinkGaugeCache;

	public FlinkMetricContainer(MetricGroup metricGroup) {
		this.baseMetricGroup = metricGroup;
		this.flinkCounterCache = new HashMap<>();
		this.flinkMeterCache = new HashMap<>();
		this.flinkDistributionGaugeCache = new HashMap<>();
		this.flinkGaugeCache = new HashMap<>();
		this.metricsContainers = new MetricsContainerStepMap();
	}

	public MetricsContainerImpl getMetricsContainer(String stepName) {
		return metricsContainers.getContainer(stepName);
	}

	/**
	 * Update this container with metrics from the passed {@link MonitoringInfo}s, and send updates
	 * along to Flink's internal metrics framework.
	 */
	public void updateMetrics(String stepName, List<MonitoringInfo> monitoringInfos) {
		getMetricsContainer(stepName).update(monitoringInfos);
		updateMetrics(stepName);
	}

	public FlinkFnApi.MetricGroupInfo getBaseMetricGroupInfo() {
		FlinkFnApi.MetricGroupInfo.Builder builder = FlinkFnApi.MetricGroupInfo.newBuilder();
		// components
		builder.addAllScopeComponents(
			Arrays.asList(baseMetricGroup.getScopeComponents()));
		// variables
		builder.putAllVariables(baseMetricGroup.getAllVariables());
		// delimiter
		builder.setDelimiter(METRIC_KEY_SEPARATOR);
		return builder.build();
	}

	/**
	 * Update Flink's internal metrics ({@link this#flinkCounterCache}) with the latest metrics for a
	 * given step.
	 */
	void updateMetrics(String stepName) {
		MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
		MetricQueryResults metricQueryResults =
			metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());
		updateCounterOrMeter(metricQueryResults.getCounters());
		updateDistributions(metricQueryResults.getDistributions());
		updateGauge(metricQueryResults.getGauges());
	}

	// todo add test and make sure compatiabilities
	private boolean isUserMetric(MetricResult metricResult) {
		MetricName metricName = metricResult.getKey().metricName();
		return (metricName instanceof MonitoringInfoMetricName) &&
			((MonitoringInfoMetricName) metricName).getUrn().equals("beam:metric:user");
	}

	private void updateCounterOrMeter(Iterable<MetricResult<Long>> counters) {
		for (MetricResult<Long> metricResult : counters) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());

			// get metric type
			ArrayList<String> scopeComponents = getNameSpaceArray(metricResult.getKey());
			if ((scopeComponents.size() % 2) != 0) {
				Meter meter;
				if (flinkMeterCache.containsKey(flinkMetricIdentifier)) {
					meter = flinkMeterCache.get(flinkMetricIdentifier);
				} else {
					int timeSpanInSeconds = Integer.valueOf(scopeComponents.get(scopeComponents.size() - 1));
					MetricGroup metricGroup = registerMetricGroup(metricResult.getKey(), baseMetricGroup);
					meter = metricGroup.meter(
						metricResult.getKey().metricName().getName(),
						new MeterView(timeSpanInSeconds));
					flinkMeterCache.put(flinkMetricIdentifier, meter);
				}

				Long update = metricResult.getAttempted();
				meter.markEvent(update);
			} else {
				Counter counter;
				if (flinkCounterCache.containsKey(flinkMetricIdentifier)) {
					counter = flinkCounterCache.get(flinkMetricIdentifier);
				} else {
					MetricGroup metricGroup = registerMetricGroup(metricResult.getKey(), baseMetricGroup);
					counter = metricGroup.counter(metricResult.getKey().metricName().getName());
					flinkCounterCache.put(flinkMetricIdentifier, counter);
				}

				Long update = metricResult.getAttempted();
				counter.inc(update - counter.getCount());
			}
		}
	}

	private void updateDistributions(Iterable<MetricResult<DistributionResult>> distributions) {
		for (MetricResult<DistributionResult> metricResult : distributions) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());
			DistributionResult update = metricResult.getAttempted();

			// update flink metric
			FlinkDistributionGauge gauge = flinkDistributionGaugeCache.get(flinkMetricIdentifier);
			if (gauge == null) {
				MetricGroup metricGroup = registerMetricGroup(metricResult.getKey(), baseMetricGroup);
				gauge = metricGroup.gauge(
					metricResult.getKey().metricName().getName(),
					new FlinkDistributionGauge(update));
				flinkDistributionGaugeCache.put(flinkMetricIdentifier, gauge);
			} else {
				gauge.update(update);
			}
		}
	}

	private void updateGauge(Iterable<MetricResult<GaugeResult>> gauges) {
		for (MetricResult<GaugeResult> metricResult : gauges) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());

			GaugeResult update = metricResult.getAttempted();

			// update flink metric
			FlinkGauge gauge = flinkGaugeCache.get(flinkMetricIdentifier);
			if (gauge == null) {
				MetricGroup metricGroup = registerMetricGroup(metricResult.getKey(), baseMetricGroup);
				gauge = metricGroup.gauge(flinkMetricIdentifier, new FlinkGauge(update));
				flinkGaugeCache.put(flinkMetricIdentifier, gauge);
			} else {
				gauge.update(update);
			}
		}
	}

	@VisibleForTesting
	static String getFlinkMetricNameString(MetricKey metricKey) {
		MetricName metricName = metricKey.metricName();
		// We use only the MetricName here, the step name is already contained
		// in the operator name which is passed to Flink's MetricGroup to which
		// the metric with the following name will be added.
		return metricName.getNamespace() + METRIC_KEY_SEPARATOR + metricName.getName();
	}

	static ArrayList<String> getNameSpaceArray(MetricKey metricKey) {
		MetricName metricName = metricKey.metricName();
		FlinkFnApi.MetricGroupInfo metricGroupInfo;
		try {
			metricGroupInfo = FlinkFnApi.MetricGroupInfo.parseFrom(
				StandardCharsets.UTF_8.encode(metricName.getNamespace()));
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}

		assert metricGroupInfo != null;
		int size = metricGroupInfo.getScopeComponentsCount();
		ArrayList<String> scopeComponents = new ArrayList<>(size);
		for (int i = 0; i < size; ++i) {
			scopeComponents.add(i, metricGroupInfo.getScopeComponents(i));
		}
		return scopeComponents;
	}

	@VisibleForTesting
	static String getFlinkMetricIdentifierString(MetricKey metricKey) {
		MetricName metricName = metricKey.metricName();
		ArrayList<String> scopeComponents = getNameSpaceArray(metricKey);
		List<String> results = scopeComponents.subList(0, scopeComponents.size() / 2);
		results.add(metricName.getName());
		return String.join(METRIC_KEY_SEPARATOR, results);
	}

	static MetricGroup registerMetricGroup(MetricKey metricKey, MetricGroup metricGroup) {
		ArrayList<String> scopeComponents = getNameSpaceArray(metricKey);
		int size = scopeComponents.size();
		List<String> metricGroupNames = scopeComponents.subList(0, size / 2);
		List<String> metricGroupTypes = scopeComponents.subList(size / 2, size);
		for (int i = 0; i < size / 2; ++i) {
			if (metricGroupTypes.get(i).equals("NormalMetricGroup")) {
				metricGroup = metricGroup.addGroup(metricGroupNames.get(i));
			} else if (metricGroupTypes.get(i).equals("KeyMetricGroup")) {
				metricGroup = metricGroup.addGroup(metricGroupNames.get(i), metricGroupNames.get(++i));
			}
		}
		return metricGroup;
	}

	/**
	 * Flink {@link Gauge} for {@link DistributionResult}.
	 */
	public static class FlinkDistributionGauge implements Gauge<DistributionResult> {

		DistributionResult data;

		FlinkDistributionGauge(DistributionResult data) {
			this.data = data;
		}

		void update(DistributionResult data) {
			this.data = data;
		}

		@Override
		public DistributionResult getValue() {
			return data;
		}
	}

	/**
	 * Flink {@link Gauge} for {@link GaugeResult}.
	 */
	public static class FlinkGauge implements Gauge<Long> {

		GaugeResult data;

		FlinkGauge(GaugeResult data) {
			this.data = data;
		}

		void update(GaugeResult update) {
			this.data = update;
		}

		@Override
		public Long getValue() {
			return data.getValue();
		}
	}
}
