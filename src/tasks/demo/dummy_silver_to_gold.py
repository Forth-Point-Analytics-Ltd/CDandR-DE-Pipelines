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
from src.transformations.to_gold.dummy.silver_to_gold import SilverToGold
from src.utils.spark_utils import (
    create_abfss_path,
    create_database_name,
    is_running_in_databricks,
)
from src.utils.file_handler import load_yaml

# COMMAND ----------
src_database, dst_database = create_database_name(
    f"{env_prefix}_silver", f"{env_prefix}_gold", git_hash
)


# COMMAND ----------
def silver_to_gold() -> None:
    table_transform_config = load_yaml(
        "./../../../src/config/dummy/silver_to_gold.yml"
    )

    SilverToGold(
        dbutils,
        task_name="silver_to_gold",
        destination_path=create_abfss_path(
            f"{env_prefix}-gold", path="spark-warehouse"
        ),
        src_database=src_database,
        dst_database=dst_database,
        overwrite_table=True,
        table_transform_config=table_transform_config,
        spark=spark,
    ).run()


# COMMAND ----------
silver_to_gold()
