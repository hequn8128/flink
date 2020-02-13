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
from pyflink.ml.param import ParamInfo, TypeConverters, Params


class ParamsTest(unittest.TestCase):

    def test_default_behavior(self):
        params = Params()

        not_optinal = ParamInfo("a", "", is_optional=False)
        with self.assertRaises(ValueError):
            params.get(not_optinal)

        # get optional without default param
        optional_without_default = ParamInfo("a", "")
        with self.assertRaises(ValueError):
            params.get(optional_without_default)

    def test_get_optional_param(self):
        param_info = ParamInfo(
            "key",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        params = Params()
        self.assertIsNone(params.get(param_info))

        val = "3"
        params.set(param_info, val)
        self.assertEqual(val, params.get(param_info))

        params.set(param_info, None)
        self.assertIsNone(params.get(param_info))

    def test_remove_contains_size_clear_is_empty(self):
        param_info = ParamInfo(
            "key",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        params = Params()
        self.assertEqual(params.size(), 0)
        self.assertTrue(params.is_empty())

        val = "3"
        params.set(param_info, val)
        self.assertEqual(params.size(), 1)
        self.assertFalse(params.is_empty())

        params_json = params.to_json()
        params_new = Params.from_json(params_json)
        self.assertEqual(params.get(param_info), val)
        self.assertEqual(params_new.get(param_info), val)

        params.clear()
        self.assertEqual(params.size(), 0)
        self.assertTrue(params.is_empty())

    def test_to_from_json(self):
        import jsonpickle

        param_info = ParamInfo(
            "keyy",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        param_info_new = jsonpickle.decode(jsonpickle.encode(param_info))
        self.assertEqual(param_info_new, param_info)

        params = Params()
        val = "3"
        params.set(param_info, val)
        params_new = Params.from_json(params.to_json())
        self.assertEqual(params_new.get(param_info), val)
