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

import unittest
from pyflink.fn_execution.flink_fn_execution_pb2 import MetricGroupInfo


class MetricTests(unittest.TestCase):

    def test_add_group(self):
        names = ["a", "b"]
        metric_group_info = MetricGroupInfo(scope_components=names)
        info_str = metric_group_info.SerializeToString().decode("utf-8")
        self.assertEqual("\na\nb", info_str)
