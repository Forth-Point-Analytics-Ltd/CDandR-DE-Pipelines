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
from src.transformations.to_bronze.dummy.csv_ingestion import (
    CSVBulkIngestionToBronze,
)
from src.utils.spark_utils import (
    create_abfss_path,
    create_database_name,
    is_running_in_databricks,
)
from src.utils.file_handler import load_yaml


# COMMAND ----------

_, dst_database = create_database_name(None, f"{env_prefix}_bronze", git_hash)


# COMMAND ----------


CSVBulkIngestionToBronze(
    dbutils,
    src_path=create_abfss_path(
        container=f"{env_prefix}-landing", path="savers"
    ),
    archive_path=create_abfss_path(
        container=f"{env_prefix}-landing", path="savers/archive"
    ),
    destination_path=create_abfss_path(
        f"{env_prefix}-bronze", path="spark-warehouse"
    ),
    task_name="savers_landing_to_bronze",
    overwrite_table=True,
    sep=",",
    table_transform_config={"savers": {"file_name": "savers_data"}},
    dst_database=dst_database,
).run()


# COMMAND ----------
