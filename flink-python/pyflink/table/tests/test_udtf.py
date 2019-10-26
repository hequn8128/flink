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
from abc import ABCMeta

from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class UserDefinedTableFunctionTests(PyFlinkStreamTableTestCase):

    def test_table_function(self):
        # test Python ScalarFunction
        self.t_env.register_function(
            "multi_emit", udtf(MultiEmit(), [DataTypes.BIGINT(), DataTypes.BIGINT()],
                               [DataTypes.BIGINT(), DataTypes.BIGINT()]))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 3, 9)], ['a', 'b', 'c'])
        t.join_lateral("multi_emit(a, b) as (x, y)") \
            .select("a, b, x, y")  \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1,1,0", "2,1,2,0", "3,3,3,0", "3,3,3,1", "3,3,3,2"])


class MultiEmit(TableFunction):
 def eval(self, x, y):
   for i in range(y):
     self.output_processor.process_outputs((x, i))


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
