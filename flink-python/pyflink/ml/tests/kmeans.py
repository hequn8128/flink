from pyflink.ml.ml_environment_factory import MLEnvironmentFactory
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from pyflink.ml import Pipeline
from pyflink.ml.feature import VectorAssembler
from pyflink.mllib.clustering import KMeans


t_env = MLEnvironmentFactory().get_default().get_batch_table_environment()
MLEnvironmentFactory().get_default().get_execution_environment().set_parallelism(1)
t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/a')) \
    .with_format(OldCsv().field_delimiter(',')
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())) \
    .with_schema(Schema()
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())) \
    .create_temporary_table('traningSource')

t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/b')) \
    .with_format(OldCsv().field_delimiter(',')
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())) \
    .with_schema(Schema()
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())) \
    .create_temporary_table('servingSource')

t_env.connect(FileSystem().path('/tmp/iris.csv')) \
    .with_format(OldCsv().field_delimiter(',')
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())
                 .field('predicate_result', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('a', DataTypes.DOUBLE())
                 .field('b', DataTypes.DOUBLE())
                 .field('predicate_result', DataTypes.BIGINT())) \
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
kmeans = KMeans() \
    .set_vector_col("features") \
    .set_k(2) \
    .set_reserved_cols(["a", "b"]) \
    .set_prediction_col("prediction_result")

# pipeline
pipeline = Pipeline().append_stage(va).append_stage(kmeans)
pipeline \
    .fit(t_env, trainTable) \
    .transform(t_env, servingTable) \
    .insert_into('mySink')

t_env.execute('KmeansTest')
