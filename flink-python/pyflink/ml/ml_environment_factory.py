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
    """
    Factory to get the MLEnvironment using a MLEnvironmentId.
    """

    def __init__(self):
        self._default_ml_environment_id = 0
        self._next_id = 1
        self._map = {}
        self.gateway = get_gateway()
        j_ml_env = self.gateway.jvm.MLEnvironmentFactory.getDefault()
        self._default_ml_env = MLEnvironment(
            ExecutionEnvironment(j_ml_env.getExecutionEnvironment()),
            StreamExecutionEnvironment(j_ml_env.getStreamExecutionEnvironment()),
            BatchTableEnvironment(j_ml_env.getBatchTableEnvironment()),
            StreamTableEnvironment(j_ml_env.getStreamTableEnvironment()))
        self._map[self._default_ml_environment_id] = self._default_ml_env

    def get(self, ml_env_id):
        """
        Get the MLEnvironment using a MLEnvironmentId.

        :param ml_env_id: the MLEnvironmentId
        :return: the MLEnvironment
        """
        if ml_env_id not in self._map:
            raise ValueError(
                "Cannot find MLEnvironment for MLEnvironmentId %s. "
                "Did you get the MLEnvironmentId by calling "
                "get_new_ml_environment_id?" % ml_env_id)
        return self._map[ml_env_id]

    def get_default(self):
        """
        Get the MLEnvironment use the default MLEnvironmentId.

        :return: the default MLEnvironment.
        """
        return self._map[self._default_ml_environment_id]

    def get_new_ml_environment_id(self):
        """
        Create a unique MLEnvironment id and register a new MLEnvironment in the factory.

        :return: the MLEnvironment id.
        """
        return self.register_ml_environment(MLEnvironment())

    def register_ml_environment(self, ml_environment):
        """
        Register a new MLEnvironment to the factory and return a new MLEnvironment id.

        :param ml_environment: the MLEnvironment that will be stored in the factory.
        :return: the MLEnvironment id.
        """
        self._map[self._next_id] = ml_environment
        self._next_id += 1
        return self._next_id - 1

    def remove(self, ml_env_id):
        """
        Remove the MLEnvironment using the MLEnvironmentId.

        :param ml_env_id: the id.
        :return: the removed MLEnvironment
        """
        if ml_env_id is None:
            raise ValueError("The environment id cannot be null.")
        # Never remove the default MLEnvironment. Just return the default environment.
        if self._default_ml_env == ml_env_id:
            return self.get_default()
        else:
            return self._map.pop(ml_env_id)

