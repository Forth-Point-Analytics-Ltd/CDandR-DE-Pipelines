# Databricks notebook source
env_prefix = dbutils.widgets.get("env_prefix")
git_hash = dbutils.widgets.get("git_hash")
db_job_id = dbutils.widgets.get("job_id")
db_run_id = dbutils.widgets.get("run_id")

# COMMAND
import os

os.environ["ENV"] = env_prefix
os.environ["DB_JOB_ID"] = db_job_id
os.environ["DB_RUN_ID"] = db_run_id

# COMMAND
from src.transformations.to_silver.savers.bronze_to_silver import (
    BronzeToSilver,
)
from src.utils.spark_utils import (
    create_abfss_path,
    create_database_name,
    is_running_in_databricks,
)
from src.utils.file_handler import load_yaml

# COMMAND ----------
src_database, dst_database = create_database_name(
    f"{env_prefix}_bronze", f"{env_prefix}_silver", git_hash
)


# COMMAND ----------
def bronze_to_silver() -> None:
    BronzeToSilver(
        dbutils,
        destination_path=create_abfss_path(
            f"{env_prefix}-silver", path="spark-warehouse"
        ),
        src_database=src_database,
        dst_database=dst_database,
        task_name="savers_bronze_to_silver",
        overwrite_table=True,
        table_transform_config={
            "savers": {
                "src_name": "savers",
                "transformation": "savers_transformation",
            }
        },
        spark=spark,
    ).run()


# COMMAND ----------
bronze_to_silver()
