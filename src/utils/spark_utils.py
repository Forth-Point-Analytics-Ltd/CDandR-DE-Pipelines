"""Spark Utils

Contains spark utility functions.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType
from typing import List, Union
from datetime import datetime
from src.config import logger
import os


def _create_db(spark: SparkSession, database_name: str, location: str) -> None:
    """Creates database.

    Args:
        spark (SparkSession): Entry point for PySpark application.
        database_name (str): Name of database.
        location (str): Location to store created database.

    Raises:
        Exception: Fails if database can't be created.
    """
    try:
        if not _db_exists(spark, database_name):
            logger.info(f"Creating db {database_name} at {location}")
            database_path = f"CREATE DATABASE `{database_name}` LOCATION '{location}{database_name}/_db/'"  # noqa
            spark.sql(database_path)
            logger.info(
                f"Successfully created db {database_name} at {location}"
            )
        else:
            logger.info(f"Database {database_name} exists")
    except Exception as error:
        logger.exception(f"Failed to create db at {location}{database_name}")
        raise error


def create_abfss_path(
    container: str, storage_account: str = None, path: str = None
) -> str:
    """Creates path to Azure Data Lake Storage Gen2.

    Args:
        container (str): Name of storage container.
        storage_account (str): Storage account name. Defaults to None.
        path (str): Path to storage account. Defaults to None.

    Returns:
        str: The constructed azure dbfs path.
    """
    # TODO: use urllib to join paths
    abfss_path = f"{os.getcwd()}/"
    if is_running_in_databricks():
        storage_account = (
            storage_account
            if storage_account
            else os.getenv("STORAGE_ACCOUNT")
        )
        abfss_path = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        )
    if path:
        if path[:1] == "/":
            path = path[1:]
        if path[-1:] == "/":
            path = path[:-1]
        abfss_path = f"{abfss_path}{path}/"
    return abfss_path


def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _save_new_table(
    df: DataFrame,
    database: str,
    table: str,
    file_format: str,
    mode="append",
    partition_by: Union[str, List[str], None] = None,
) -> None:
    """Saves PySpark dataframe as a table.

    Args:
        df (DataFrame): PySpark dataframe.
        database (str): Name of database.
        table (str): Name of table.
        file_format (str): Name of file format the dataframe is saved as.
        mode (str): Mode in which table should be created in. Possible values: `error`, `overwrite`, `append`, `ignore` .Defaulted to `append`.
        partition_by (Union[str, list[str], None]): Splits data into partitions using the provided columns. Defaults to None.
    """
    logger.info(f"Save table to {database}.{table}")
    df.write.format(file_format).saveAsTable(
        f"{database}.{table}", mode=mode, partitionBy=partition_by
    )
    logger.info(f"Table successfully saved :{database}.{table}")


def _db_exists(spark: SparkSession, database: str) -> None:
    return spark.catalog._jcatalog.databaseExists(database)


def save(
    spark: SparkSession,
    df: DataFrame,
    destination_path: str,
    database_name: str,
    table_name: str,
    file_format: str,
    overwrite: bool,
    optimize_table: bool = False,
    partition_by: Union[List[str], str, None] = None,
) -> None:
    """Create a database if it doesn't exist. Append a timestamp to the dataframe before saving as a table.
    Optional optimization of table.

    Args:
        spark (SparkSession): Entry point for PySpark application.
        df (DataFrame): PySpark dataframe.
        destination_path (str): Location to save dataframe.
        database_name (str): Name of database.
        table_name (str): Name of table.
        file_format (str): Format in which to save table.
        overwrite (bool): True overwrites current table. False appends.
        optimize_table (bool): Defaults to False.
        partition_by (Union[str, list[str], None]): Partitions table based on one or more columns. Defaults to None.

    Examples:
        >>> spark=SparkSession.builder.getOrCreate()
            df=Spark.createDataFrame([(0,1,2),(3,4,5)])
            save(
                spark=spark,
                df=df,
                destination_path="destination",
                database_name="my_database",
                table_name="my_table",
                file_format="delta",
                overwrite=False,
                optimize_table=False,
                partition_by=["year", "month"]
            )
    """
    df = df.withColumn(
        "processing_ts", lit(datetime.now()).cast(TimestampType())
    )

    _create_db(spark, database_name, destination_path)
    mode = "overwrite" if overwrite else "append"
    logger.info(f"Preparing to write {table_name}")
    _save_new_table(
        df,
        database_name,
        table_name,
        file_format,
        mode=mode,
        partition_by=partition_by,
    )
    if optimize_table:
        logger.info("Applying table optimizer")
        optimize_query = f"OPTIMIZE {database_name}.{table_name}"
        spark.sql(optimize_query)


def create_database_name(src_database, dst_database, git_hash=None):
    """Create database name based on there being a git hash or not.

    Args:
        src_database (str): Source Database
        dst_database (str): Destination Database
        git_hash (str, optional): Suffix if provided. Defaults to None.

    Returns:
        str: Name of the database suffixed if provided.
    """
    if git_hash is not None:
        src_database = f"{src_database}_{git_hash}"
        dst_database = f"{dst_database}_{git_hash}"
    return src_database, dst_database
