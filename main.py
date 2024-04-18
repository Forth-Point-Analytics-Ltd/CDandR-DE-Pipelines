from src.spark_config import get_spark_session
from src.tasks.demo.dummy_landing_to_bronze import dummy_to_bronze
from src.tasks.demo.dummy_bronze_to_silver import bronze_to_silver
from src.tasks.demo.dummy_silver_to_gold import silver_to_gold
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

if __name__ == "__main__":
    dummy_to_bronze()
    bronze_to_silver()
    silver_to_gold()
    spark: SparkSession = get_spark_session("dummy_session")
    spark.table("gold.person_airport_location").where(
        (sf.col("person_id").isNotNull())
        & (sf.col("airport_id").isNotNull())
        & (sf.col("country_id").isNotNull())
    ).show()
