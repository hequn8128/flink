from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from pyflink.ml import Pipeline
from pyflink.ml.feature import VectorAssembler
from pyflink.ml.ml_environment_factory import MLEnvironmentFactory
from pyflink.mllib.clustering import KMeans

t_env = MLEnvironmentFactory().get_default().get_batch_table_environment()
MLEnvironmentFactory().get_default().get_execution_environment().set_parallelism(1)

t_env.sql_update(
    """
    CREATE TABLE sourceTable(
        sepal_length DOUBLE,
        sepal_width DOUBLE,
        petal_length DOUBLE,
        petal_width DOUBLE,
        category VARCHAR
	) WITH (
	  'connector.type' = 'filesystem',
	  'connector.path' = '/Users/hequn.chq/Downloads/iris.csv',
	  'format.type' = 'csv'
	)
    """
)

t_env.sql_update(
    """
    CREATE TABLE kmeansResults(
        sepal_length DOUBLE,
        sepal_width DOUBLE,
        petal_length DOUBLE,
        petal_width DOUBLE,
        category VARCHAR,
        prediction_result BIGINT 
	) WITH (
	  'connector.type' = 'filesystem',
	  'connector.path' = '/Users/hequn.chq/Downloads/kmeans_results_1.csv',
	  'format.type' = 'csv'
	)
    """
)

sourceTable = t_env.from_path('sourceTable')

# transformer
# va = VectorAssembler(selected_cols=["", "b"], output_col="features")
va = VectorAssembler()\
    .set_selected_cols(["sepal_length", "sepal_width", "petal_length", "petal_width"])\
    .set_output_col("features")

# estimator
# kmeans = KMeans(vector_col="features", k=2, reserved_cols=["a", "b"], prediction_col="prediction_result")
kmeans = KMeans() \
    .set_vector_col("features") \
    .set_k(3) \
    .set_reserved_cols(["sepal_length", "sepal_width", "petal_length", "petal_width", "category"]) \
    .set_prediction_col("prediction_result")

# pipeline
pipeline = Pipeline().append_stage(va).append_stage(kmeans)
pipeline \
    .fit(t_env, sourceTable) \
    .transform(t_env, sourceTable) \
    .insert_into('kmeansResults')

t_env.execute('KmeansTest')
