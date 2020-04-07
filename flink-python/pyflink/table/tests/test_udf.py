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
import datetime

import pytz
import unittest

from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, \
    PyFlinkBlinkStreamTableTestCase, PyFlinkBlinkBatchTableTestCase, \
    PyFlinkBatchTableTestCase


class UserDefinedFunctionTests(object):

    def test_chaining_scalar_function(self):
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function("add", add)

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1,), (2,)], ['a'])
        t.select("add_one(a)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2", "3"])


# decide whether two floats are equal
def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


class PyFlinkStreamUserDefinedFunctionTests(UserDefinedFunctionTests,
                                            PyFlinkStreamTableTestCase):
    pass


class PyFlinkBatchUserDefinedFunctionTests(PyFlinkBatchTableTestCase):

    def test_chaining_scalar_function(self):
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function("add", add)

        t = self.t_env.from_elements([(1, 2, 1), (2, 5, 2), (3, 1, 3)], ['a', 'b', 'c'])\
            .select("add(add_one(a), subtract_one(b)), c, 1")

        result = self.collect(t)
        self.assertEqual(result, ["3,1,1", "7,2,1", "4,3,1"])


class PyFlinkBlinkStreamUserDefinedFunctionTests(UserDefinedFunctionTests,
                                                 PyFlinkBlinkStreamTableTestCase):
    def test_deterministic(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(add_one._deterministic)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)
        self.assertFalse(add_one._deterministic)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(subtract_one._deterministic)

        with self.assertRaises(ValueError, msg="Inconsistent deterministic: False and True"):
            udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)

        self.assertTrue(add._deterministic)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), deterministic=False)
        def non_deterministic_udf(i):
            return i

        self.assertFalse(non_deterministic_udf._deterministic)

    def test_name(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("<lambda>", add_one._name)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), name="add_one")
        self.assertEqual("add_one", add_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("SubtractOne", subtract_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(),
                           name="subtract_one")
        self.assertEqual("subtract_one", subtract_one._name)

        self.assertEqual("add", add._name)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), name="named")
        def named_udf(i):
            return i

        self.assertEqual("named", named_udf._name)

    def test_abc(self):
        class UdfWithoutEval(ScalarFunction):
            def open(self, function_context):
                pass

        with self.assertRaises(
                TypeError,
                msg="Can't instantiate abstract class UdfWithoutEval with abstract methods eval"):
            UdfWithoutEval()

    def test_invalid_udf(self):
        class Plus(object):
            def eval(self, col):
                return col + 1

        with self.assertRaises(
                TypeError,
                msg="Invalid function: not a function or callable (__call__ is not defined)"):
            # test non-callable function
            self.t_env.register_function(
                "non-callable-udf", udf(Plus(), DataTypes.BIGINT(), DataTypes.BIGINT()))

    def test_data_types_only_supported_in_blink_planner(self):
        timezone = self.t_env.get_config().get_local_timezone()
        local_datetime = pytz.timezone(timezone).localize(
            datetime.datetime(1970, 1, 1, 0, 0, 0, 123000))

        def local_zoned_timestamp_func(local_zoned_timestamp_param):
            assert local_zoned_timestamp_param == local_datetime, \
                'local_zoned_timestamp_param is wrong value %s !' % local_zoned_timestamp_param
            return local_zoned_timestamp_param

        self.t_env.register_function(
            "local_zoned_timestamp_func",
            udf(local_zoned_timestamp_func,
                [DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)],
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))

        table_sink = source_sink_utils.TestAppendSink(
            ['a'], [DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements(
            [(local_datetime,)],
            DataTypes.ROW([DataTypes.FIELD("a", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))]))

        t.select("local_zoned_timestamp_func(local_zoned_timestamp_func(a))") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1970-01-01T00:00:00.123Z"])


class PyFlinkBlinkBatchUserDefinedFunctionTests(UserDefinedFunctionTests,
                                                PyFlinkBlinkBatchTableTestCase):
    pass


@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    return i + j


class SubtractOne(ScalarFunction):

    def eval(self, i):
        return i - 1


class Subtract(ScalarFunction, unittest.TestCase):

    def open(self, function_context):
        self.subtracted_value = 1
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def eval(self, i):
        # counter
        self.counter.inc(i)
        self.counter_sum += i
        self.assertEqual(self.counter_sum, self.counter.get_count())
        return i - self.subtracted_value


class CallablePlus(object):

    def __call__(self, col):
        return col + 1


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
