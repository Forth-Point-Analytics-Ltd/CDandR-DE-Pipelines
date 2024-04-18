# Databricks notebook source
env_prefix = dbutils.widgets.get("env_prefix")
db_job_id = dbutils.widgets.get("job_id")
db_run_id = dbutils.widgets.get("run_id")

# COMMAND
import os

os.environ["ENV"] = env_prefix
os.environ["DB_JOB_ID"] = db_job_id
os.environ["DB_RUN_ID"] = db_run_id

# COMMAND ----------
from src.transformations.to_landing.dummy.unzip_dummy_to_landing import (
    dummy_to_landing,
)

# COMMAND ----------

dummy_to_landing(dbutils, env_prefix)
