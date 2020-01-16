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

from pyflink.ml.ml_environment_factory import MLEnvironmentFactory
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from pyflink.ml import Pipeline
from pyflink.ml.feature import VectorAssembler
from pyflink.mllib.clustering import KMeans
from pyflink.testing.test_case_utils import PyFlinkTestCase


class KmeansTest(PyFlinkTestCase):

    def test_kmeans(self):
        t_env = MLEnvironmentFactory().get_default().get_batch_table_environment()
        MLEnvironmentFactory().get_default().get_execution_environment().set_parallelism(1)
        t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/a'))\
            .with_format(OldCsv().field_delimiter(',')
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE())) \
            .with_schema(Schema()
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE()))\
            .create_temporary_table('traningSource')

        t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/b'))\
            .with_format(OldCsv().field_delimiter(',')
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE())) \
            .with_schema(Schema()
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE()))\
            .create_temporary_table('servingSource')

        t_env.connect(FileSystem().path('/tmp/iris.csv'))\
            .with_format(OldCsv().field_delimiter(',')
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE())
                         .field('predicate_result', DataTypes.BIGINT())) \
            .with_schema(Schema()
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE())
                         .field('predicate_result', DataTypes.BIGINT()))\
            .create_temporary_table('mySink')

        trainTable = t_env.from_path('traningSource')
        servingTable = t_env.from_path('servingSource')

        # transformer
        va = VectorAssembler(selected_cols=["a", "b"], output_col="features")
        # va = VectorAssembler()\
        #     .set_selected_cols(["a", "b"])\
        #     .set_output_col("features")

        # estimator
        # kmeans = KMeans(vector_col="features", k=2, reserved_cols=["a", "b"], prediction_col="prediction_result")
        kmeans = KMeans()\
            .set_vector_col("features")\
            .set_k(2)\
            .set_reserved_cols(["a", "b"])\
            .set_prediction_col("prediction_result")

        # pipeline
        pipeline = Pipeline().append_stage(va).append_stage(kmeans)
        pipeline\
            .fit(t_env, trainTable)\
            .transform(t_env, servingTable)\
            .insert_into('mySink')

        t_env.execute('KmeansTest')



