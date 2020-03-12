################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import abc
from apache_beam.metrics.metric import Metrics
from typing import Callable

from pyflink.fn_execution.flink_fn_execution_pb2 import MetricGroupInfo


class MetricGroup(abc.ABC):

    def _get_metric_group_names_and_types(self) -> ([], []):
        pass

    def _get_delimiter(self) -> str:
        pass

    def _get_namespace(self, time=None):
        (names, metric_group_type) = self._get_metric_group_names_and_types()
        names.extend(metric_group_type)
        if time is not None:
            names.append(str(time))
        names.extend(str(60))
        # use MetricGroupInfo to pass names and types info from Python to Java
        return MetricGroupInfo(scope_components=names).SerializeToString().decode("utf-8")

    def counter(self, name: str) -> 'Counter':
        """
        Registers a new `Counter` with Flink.
        """
        return Counter(Metrics.counter(self._get_namespace(), name))

    def gauge(self, name: str) -> 'Gauge':
        """
        Registers a new `Gauge` with Flink.
        """
        return Gauge(Metrics.gauge(self._get_namespace(), name))

    def new_gauge(self, name: str, obj: 'NewGauge') -> 'Gauge':
        """
        Registers a new `Gauge` with Flink.
        """
        pass

    def new_gauge2(self, name: str, obj: Callable[[], int]) -> None:
        """
        Registers a new `Gauge` with Flink.
        """
        pass

    def meter(self, name: str, time_span_in_seconds: int = 60) -> 'Meter':
        """
        Registers a new `Meter` with Flink.
        """
        # There is no meter type in Beam, use counter to implement meter
        return Meter(
            Metrics.counter(self._get_namespace(time_span_in_seconds), name),
            time_span_in_seconds)

    def distribution(self, name: str) -> 'Distribution':
        """
        Registers a new `Distribution` with Flink.
        """
        return Distribution(Metrics.distribution(self._get_namespace(), name))

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        """
        if extra is not None, creates a new key-value MetricGroup pair. The key group
        is added to this groups sub-groups, while the value group is added to the key
        group's sub-groups. This method returns the value group.

        The only difference between calling this method and
        `group.add_group(key).add_group(value)` is that get_all_variables()
        of the value group return an additional `"<key>"="value"` pair.
        """

        if extra is None:
            return NormalMetricGroup(self, name)
        else:
            return KeyValueMetricGroup(self, name, extra)

    def get_scope_components(self) -> []:
        """
        Gets the scope as an array of the scope components, for example
        `["host-7", "taskmanager-2", "window_word_count", "my-mapper"]`
        """
        pass

    def get_all_variables(self) -> map:
        """
        Returns a map of all variables and their associated value, for example
        `{"<host>"="host-7", "<tm_id>"="taskmanager-2"}`
        """
        pass

    def get_metric_identifier(self, metric_name: str) -> str:
        """
        Returns the fully qualified metric name, for example
        `host-7.taskmanager-2.window_word_count.my-mapper.metricName`
        """
        identifier_array = self.get_scope_components()
        identifier_array.append(metric_name)
        return self._get_delimiter().join(identifier_array)


class BaseMetricGroup(MetricGroup):

    def __init__(self, variables, scope_components, delimiter):
        self._variables = variables
        self._scope_components = scope_components
        self._delimiter = delimiter
        self._flink_gauge = {}
        self._flink_gauge2 = {}
        self._beam_gauge = {}

    def _get_metric_group_names_and_types(self) -> ([], []):
        return [], []

    def _get_delimiter(self) -> str:
        return self._delimiter

    def get_scope_components(self) -> []:
        ret = []
        ret.extend(self._scope_components)
        return ret

    def get_all_variables(self) -> map:
        ret = {}
        ret.update(self._variables)
        return ret

    def new_gauge(self, name: str, obj: 'NewGauge') -> None:
        self._flink_gauge[name] = obj
        self._beam_gauge[name] = Metrics.gauge(super(BaseMetricGroup, self)._get_namespace(), name)

    def new_gauge2(self, name: str, obj) -> None:
        self._flink_gauge2[name] = obj
        self._beam_gauge[name] = Metrics.gauge(super(BaseMetricGroup, self)._get_namespace(), name)


class NormalMetricGroup(MetricGroup):

    def __init__(self, parent, name):
        self._parent = parent
        self._name = name
        self._flink_gauge = {}
        self._flink_gauge2 = {}
        self._beam_gauge = {}

    def _get_metric_group_names_and_types(self):
        names, types = self._parent._get_metric_group_names_and_types()
        names.append(self._name)
        types.append('NormalMetricGroup')
        return names, types

    def get_scope_components(self) -> []:
        scope_compoents = self._parent.get_scope_components()
        scope_compoents.append(self._name)
        return scope_compoents

    def get_all_variables(self) -> map:
        return self._parent.get_all_variables()

    def _get_delimiter(self) -> str:
        return self._parent._get_delimiter()

    def new_gauge(self, name: str, obj: 'NewGauge') -> None:
        self._flink_gauge[name] = obj
        self._beam_gauge[name] = Metrics.gauge(self._get_namespace(), name)

    def new_gauge2(self, name: str, obj) -> None:
        self._flink_gauge2[name] = obj
        self._beam_gauge[name] = Metrics.gauge(super(BaseMetricGroup, self)._get_namespace(), name)


class KeyValueMetricGroup(MetricGroup):

    def __init__(self, parent, key_name, value_name):
        self._parent = parent
        self._key_name = key_name
        self._value_name = value_name
        self._flink_gauge = {}
        self._flink_gauge2 = {}
        self._beam_gauge = {}

    def _get_metric_group_names_and_types(self):
        names, types = self._parent._get_metric_group_names_and_types()
        names.extend([self._key_name, self._value_name])
        types.extend(['KeyMetricGroup', 'ValueMetricGroup'])
        return names, types

    def _get_delimiter(self) -> str:
        return self._parent._get_delimiter()

    def get_scope_components(self) -> []:
        scope_compoents = self._parent.get_scope_components()
        scope_compoents.extend([self._key_name, self._value_name])
        return scope_compoents

    def get_all_variables(self) -> map:
        variables = self._parent.get_all_variables()
        variables[self._key_name] = self._value_name
        return variables

    def new_gauge(self, name: str, obj: 'NewGauge') -> None:
        self._flink_gauge[name] = obj
        self._beam_gauge[name] = Metrics.gauge(self._get_namespace(), name)

    def new_gauge2(self, name: str, obj) -> None:
        self._flink_gauge2[name] = obj
        self._beam_gauge[name] = Metrics.gauge(super(BaseMetricGroup, self)._get_namespace(), name)


class Metric(object):
    """Base interface of a metric object."""
    pass


class Counter(Metric):
    """Counter metric interface. Allows a count to be incremented/decremented
    during pipeline execution."""

    def __init__(self, inner_counter):
        self._inner_counter = inner_counter

    def inc(self, n=1):
        self._inner_counter.inc(n)

    def dec(self, n=1):
        self.inc(-n)


class Gauge(Metric):
    """Gauge Metric interface.

    Allows tracking of the latest value of a variable during pipeline
    execution."""

    def __init__(self, inner_gauge):
        self._inner_gauge = inner_gauge

    def set(self, value):
        self._inner_gauge.set(value)


class NewGauge(Metric):

    def get_value(self):
        pass



class Distribution(Metric):
    """Distribution Metric interface.

    Allows statistics about the distribution of a variable to be collected during
    pipeline execution."""

    def __init__(self, inner_distribution):
        self._inner_distribution = inner_distribution

    def update(self, value):
        self._inner_distribution.update(value)


class Meter(Metric):
    """Meter Metric interface.

    Metric for measuring throughput."""

    def __init__(self, inner_counter, time_span_in_seconds=60):
        self._inner_counter = inner_counter
        self._time_span_in_seconds = time_span_in_seconds

    def mark_event(self, value=1):
        self._inner_counter.inc(value)

    def get_time_span_in_seconds(self):
        return self._time_span_in_seconds
