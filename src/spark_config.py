from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.utils.spark_utils import is_running_in_databricks


def get_spark_session(name: str) -> SparkSession:
    """The session that runs the main spark application.

    Args:
        name (str): The name of the spark application.

    Returns:
        SparkSession: A spark session configured with delta.
    """
    builder = SparkSession.builder.appName(name)
    if not is_running_in_databricks():
        builder = configure_spark_with_delta_pip(
            builder
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.0")
            .config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )


    return builder.getOrCreate()
