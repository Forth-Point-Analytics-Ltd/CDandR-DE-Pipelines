# Databricks notebook source
import os

# COMMAND ----------
env_prefix = dbutils.widgets.get("env_prefix")
git_hash = dbutils.widgets.get("git_hash")

# COMMAND ----------
bronze = f"{env_prefix}_bronze_{git_hash}"
silver = f"{env_prefix}_silver_{git_hash}"
gold = f"{env_prefix}_gold_{git_hash}"

db_list = [bronze, silver, gold]


# COMMAND ----------

for db in db_list:
    if  spark.catalog._jcatalog.databaseExists(db):
        spark.sql(f"drop database {db} cascade")
    else:
        print(f"Database {db} does not exist nothing to drop")

# COMMAND ---------
dst = f"abfss://{env_prefix}-landing@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/dummy/archive/"
dbutils.fs.rm(dst, True)
