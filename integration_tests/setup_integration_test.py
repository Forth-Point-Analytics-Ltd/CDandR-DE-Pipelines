# Databricks notebook source
import os

# COMMAND ----------

env_prefix = dbutils.widgets.get("env_prefix")

# COMMAND ----------

src = f"abfss://raw-data-test@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/dummy/"
dst = f"abfss://{env_prefix}-landing@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/dummy/"
for file_obj in dbutils.fs.ls(src):
    dbutils.fs.cp(file_obj.path, f"{dst}{file_obj.name}")
