from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from pyflink.ml import Pipeline
from pyflink.ml.feature import VectorAssembler
from pyflink.ml.ml_environment_factory import MLEnvironmentFactory
from pyflink.mllib.clustering import KMeans

t_env = MLEnvironmentFactory().get_default().get_batch_table_environment()
MLEnvironmentFactory().get_default().get_execution_environment().set_parallelism(1)

t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/iris.csv')) \
    .with_format(OldCsv().field_delimiter(',')
                 .field('sepal_length', DataTypes.DOUBLE())
                 .field('sepal_width', DataTypes.DOUBLE())
                 .field('petal_length', DataTypes.DOUBLE())
                 .field('petal_width', DataTypes.DOUBLE())
                 .field('category', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('sepal_length', DataTypes.DOUBLE())
                 .field('sepal_width', DataTypes.DOUBLE())
                 .field('petal_length', DataTypes.DOUBLE())
                 .field('petal_width', DataTypes.DOUBLE())
                 .field('category', DataTypes.STRING())) \
    .create_temporary_table('sourceTable')

t_env.connect(FileSystem().path('/Users/hequn.chq/Downloads/kmeans_results.csv')) \
    .with_format(OldCsv().field_delimiter(',')
                 .field('sepal_length', DataTypes.DOUBLE())
                 .field('sepal_width', DataTypes.DOUBLE())
                 .field('petal_length', DataTypes.DOUBLE())
                 .field('petal_width', DataTypes.DOUBLE())
                 .field('category', DataTypes.STRING())
                 .field('prediction_result', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('sepal_length', DataTypes.DOUBLE())
                 .field('sepal_width', DataTypes.DOUBLE())
                 .field('petal_length', DataTypes.DOUBLE())
                 .field('petal_width', DataTypes.DOUBLE())
                 .field('category', DataTypes.STRING())
                 .field('prediction_result', DataTypes.BIGINT())) \
    .create_temporary_table('kmeansResults')

sourceTable = t_env.from_path('sourceTable')

# transformer
va = VectorAssembler()\
    .set_selected_cols(["sepal_length",
                        "sepal_width", "petal_length", "petal_width"])\
    .set_output_col("features")

# estimator
kmeans = KMeans() \
    .set_vector_col("features") \
    .set_k(3) \
    .set_reserved_cols(["sepal_length", "sepal_width",
                        "petal_length", "petal_width", "category"]) \
    .set_prediction_col("prediction_result")

# pipeline
pipeline = Pipeline().append_stage(va).append_stage(kmeans)
pipeline \
    .fit(t_env, sourceTable) \
    .transform(t_env, sourceTable) \
    .insert_into('KmeansResults')

t_env.execute('KmeansTest')
