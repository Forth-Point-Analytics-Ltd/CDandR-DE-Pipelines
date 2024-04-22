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
from src.transformations.to_landing.savers.savers_web_scraper import (
    SaversWebScraper,
)
from src.utils.spark_utils import (
    create_abfss_path,
)


# COMMAND ----------
SaversWebScraper(
    "https://www.savers.co.uk",
    "brand_competitors.json",
    create_abfss_path(container="dev-landing", path="savers/savers_data.csv"),
).run()
