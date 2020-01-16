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
from pyflink.testing.test_case_utils import PyFlinkTestCase

from pyflink.ml import Transformer, Estimator, Model, Pipeline
from pyflink.ml.param.colname import *
from pyflink import keyword
from pyflink.testing import source_sink_utils


class PythonTransformer(Transformer, HasSelectedCols, HasOutputCol):
    @keyword
    def __init__(self, *, selected_cols=None, output_col=None):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        input_columns = self.get_selected_cols()
        expr = "+".join(input_columns)
        expr = expr + " as " + self.get_output_col()
        return table.add_columns(expr)


class PythonEstimator(Estimator, HasVectorCol, HasPredictionCol, HasReservedCols):

    def __init__(self):
        super().__init__()

    def fit(self, table_env, table):
        return PythonModel(table_env, table.select("max(features) as max_sum"))


class PythonModel(Model):

    def __init__(self, table_env, model_data_table):
        self.model_data_table = model_data_table
        self.table_env = table_env
        self.max_sum = 0
        self.load_model(table_env)

    def load_model(self, table_env):
        table_sink = source_sink_utils.TestRetractSink(["max_sum"], [DataTypes.DOUBLE()])
        table_env.register_table_sink("Results", table_sink)
        self.model_data_table.insert_into("Results")
        table_env.execute("load model")
        actual = source_sink_utils.results()
        self.max_sum = actual.apply(0)

    def transform(self, table_env, table):
        return table\
            .add_columns("features > {} as predicate_result".format(self.max_sum))\
            .select("a, b, predicate_result")


class PythonPipelineTest(PyFlinkTestCase):

    def test_pipeline(self):
        t_env = MLEnvironmentFactory().get_default().get_stream_table_environment()
        MLEnvironmentFactory().get_default().get_stream_execution_environment().set_parallelism(1)
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
                         .field('predicate_result', DataTypes.BOOLEAN())) \
            .with_schema(Schema()
                         .field('a', DataTypes.DOUBLE())
                         .field('b', DataTypes.DOUBLE())
                         .field('predicate_result', DataTypes.BOOLEAN()))\
            .create_temporary_table('mySink')

        trainTable = t_env.from_path('traningSource')
        servingTable = t_env.from_path('servingSource')

        # transformer
        transformer = PythonTransformer(selected_cols=["a", "b"], output_col="features")

        # estimator
        estimator = PythonEstimator()\
            .set_vector_col("features")\
            .set_reserved_cols(["a", "b"])\
            .set_prediction_col("predicate_result")

        # pipeline
        pipeline = Pipeline().append_stage(transformer).append_stage(estimator)
        pipeline\
            .fit(t_env, trainTable)\
            .transform(t_env, servingTable)\
            .insert_into('mySink')
        #
        t_env.execute('KmeansTest')

