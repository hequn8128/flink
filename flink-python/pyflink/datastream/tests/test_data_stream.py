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
from pyflink.datastream.udfs import KeySelector
from pyflink.datastream.udfs import MapFunction, FlatMapFunction
from pyflink.testing.test_case_utils import PyFlinkTestCase


class DataStreamTests(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def test_map_function_without_data_types(self):
        test_sink = DataStreamTestCollectSink(True)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.map(MyMapFunction()).add_sink(test_sink)
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
            return value[0], len(value[0]), value[1]

        ds.map(map_func, type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()])) \
            .add_sink(test_sink)
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
            .add_sink(test_sink)
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
            .add_sink(test_sink)
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
            .add_sink(test_sink)
        self.env.execute('flat_map_test')
        result = test_sink.collect()
        expected = ['a,0', 'bdc,2', 'deeefg,4']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_key_by(self):
        test_sink = DataStreamTestCollectSink(True)
        element_collection = [('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)]
        self.env.set_parallelism(1)
        ds = self.env.from_collection(element_collection,
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        class AssertKeyMapFunction(MapFunction):
            def __init__(self):
                self.pre = None

            def map(self, value):
                if value[0] == 'b':
                    assert self.pre == 'a'
                if value[0] == 'd':
                    assert self.pre == 'c'
                self.pre = value[0]
                return value

        ds.key_by(MyKeySelector()).map(AssertKeyMapFunction()).add_sink(test_sink)
        self.env.execute('key_by_test')
        result = test_sink.collect()
        expected = ["<Row('a', 0)>", "<Row('b', 0)>", "<Row('c', 1)>", "<Row('d', 1)>",
                    "<Row('e', 2)>"]
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_key_by_map(self):
        test_sink_1 = DataStreamTestCollectSink(True)
        test_sink_2 = DataStreamTestCollectSink(True)
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector())

        class AssertKeyMapFunction(MapFunction):
            def __init__(self):
                self.pre = None

            def map(self, value):
                if value[0] == 'b':
                    assert self.pre == 'a'
                if value[0] == 'd':
                    assert self.pre == 'c'
                self.pre = value[0]
                return value

        keyed_stream.map(AssertKeyMapFunction()).add_sink(test_sink_1)
        keyed_stream.map(AssertKeyMapFunction()).add_sink(test_sink_2)
        self.env.execute('key_by_test')
        test_sink_1.collect()
        test_sink_2.collect()


class MyMapFunction(MapFunction):

    def map(self, value):
        result = (value[0], len(value[0]), value[1])
        return result


class MyFlatMapFunction(FlatMapFunction):

    def flat_map(self, value):
        if value[1] % 2 == 0:
            yield value


class MyKeySelector(KeySelector):
    def get_key(self, value):
        return value[1]
