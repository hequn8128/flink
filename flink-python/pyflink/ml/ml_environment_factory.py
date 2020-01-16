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

from pyflink.java_gateway import get_gateway
from pyflink.ml.ml_environment import MLEnvironment
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import BatchTableEnvironment, StreamTableEnvironment


def singleton(cls):
    instance = [None]

    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]

    return wrapper


@singleton
class MLEnvironmentFactory:

    def __init__(self):
        self.gateway = get_gateway()
        j_ml_env = self.gateway.jvm.MLEnvironmentFactory.getDefault()
        self._default_ml_env = MLEnvironment(
            ExecutionEnvironment(j_ml_env.getExecutionEnvironment()),
            StreamExecutionEnvironment(j_ml_env.getStreamExecutionEnvironment()),
            BatchTableEnvironment(j_ml_env.getBatchTableEnvironment()),
            StreamTableEnvironment(j_ml_env.getStreamTableEnvironment()))

    def get(self, ml_env_id):
        return self.j_ml_env.get(ml_env_id)

    def get_default(self):
        return self._default_ml_env

    def get_new_ml_environment_id(self):
        return self.j_ml_env.getNewMLEnvironmentId()

    # def register_ml_environment(self, ml_environment):
    #     return self.j_ml_env.registerMLEnvironment(ml_environment.j_ml_environment)
