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
from pyflink.common.typeinfo import Types

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.tests.test_util import DataStreamTestCollectSink
from pyflink.datastream.udfs import MapFunction, FlatMapFunction
from pyflink.testing.test_case_utils import PyFlinkTestCase


class DataStreamTests(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def test_map_function_without_data_types(self):
        test_sink = DataStreamTestCollectSink(True)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.map(MyMapFunction()) \
            ._j_data_stream.addSink(test_sink._j_data_stream_test_collect_sink)
        self.env.execute('map_function_test')
        result = test_sink.collect()
        expected = ["('ab', 2, 1)", "('bdc', 3, 2)", "('cfgs', 4, 3)", "('deeefg', 6, 4)"]
        expected.sort()
        result.sort()
        self.assertEqual(expected, result)

    def test_map_function_with_data_types(self):
        test_sink = DataStreamTestCollectSink(False)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        def map_func(value):
            result = (value[0], len(value[0]), value[1])
            return result

        ds.map(map_func, type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()])) \
            ._j_data_stream.addSink(test_sink._j_data_stream_test_collect_sink)
        self.env.execute('map_function_test')
        result = test_sink.collect()
        expected = ['ab,2,1', 'bdc,3,2', 'cfgs,4,3', 'deeefg,6,4']
        expected.sort()
        result.sort()
        self.assertEqual(expected, result)

    def test_map_function_with_data_types_and_function_object(self):
        test_sink = DataStreamTestCollectSink(False)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        ds.map(MyMapFunction(), type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()])) \
            ._j_data_stream.addSink(test_sink._j_data_stream_test_collect_sink)
        self.env.execute('map_function_test')
        result = test_sink.collect()
        expected = ['ab,2,1', 'bdc,3,2', 'cfgs,4,3', 'deeefg,6,4']
        expected.sort()
        result.sort()
        self.assertEqual(expected, result)

    def test_flat_map_function(self):
        test_sink = DataStreamTestCollectSink(False)
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.flat_map(MyFlatMapFunction(), type_info=Types.ROW([Types.STRING(), Types.INT()])) \
            ._j_data_stream.addSink(test_sink._j_data_stream_test_collect_sink)
        self.env.execute('flat_map_test')
        result = test_sink.collect()
        expected = ['a,0', 'bdc,2', 'deeefg,4']
        result.sort()
        expected.sort()

        self.assertEqual(expected, result)

    def test_flat_map_function_with_function_object(self):
        test_sink = DataStreamTestCollectSink(False)
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        def flat_map(value):
            if value[1] % 2 == 0:
                yield value

        ds.flat_map(flat_map, type_info=Types.ROW([Types.STRING(), Types.INT()])) \
            ._j_data_stream.addSink(test_sink._j_data_stream_test_collect_sink)
        self.env.execute('flat_map_test')
        result = test_sink.collect()
        expected = ['a,0', 'bdc,2', 'deeefg,4']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)


class MyMapFunction(MapFunction):

    def map(self, value):
        result = (value[0], len(value[0]), value[1])
        return result


class MyFlatMapFunction(FlatMapFunction):

    def flat_map(self, value):
        if value[1] % 2 == 0:
            yield value
