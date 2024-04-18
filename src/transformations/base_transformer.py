from abc import ABCMeta, abstractmethod
from typing import List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from src.spark_config import get_spark_session
from src.utils.spark_utils import save
from src.config import logger


class BaseTransformer(metaclass=ABCMeta):
    """Abstract class that contains functionality that can be used commonly between
    the data pipeline layers.

    New class objects can be initialised by inheriting from the parent class. After
    inheriting, the child class should contain the transform() method.

    Args:
        metaclass (class, ABCMeta): Allows for a mix of concrete and abstract classes. Defaults to ABCMeta.
    """

    def __init__(
        self,
        dbutils,
        task_name: str,
        destination_path: str,
        overwrite_table=False,
        destination_file_format: str = "DELTA",
        spark: Optional[SparkSession] = None,
    ) -> None:
        """Constructor for the BaseTransformer

        Args:
            dbutils (dbutils): The databricks environment utility package.
            task_name (str): The name the spark application will take for its duration.
            destination_path (str): The destination path the where the database will live.
            database_name (str): The name of the database.
            overwrite_table (bool, optional): Set whether or not to overwrite table. Defaults to False.
            destination_file_format (str, optional): The file format the dataframe should be saved as. Defaults to "DELTA".
            spark (Optional[SparkSession], optional): The main spark instance. Defaults to None.

        Attributes:
            cached_tables (List[DataFrame]): The list of dataframes that have been cached.
        """
        self.dbutils = dbutils
        self.destination_path = destination_path
        self.destination_file_format = destination_file_format
        self.task_name = task_name
        self.spark = get_spark_session(self.task_name) if not spark else spark
        self.overwrite_table = overwrite_table
        self.cached_tables = []

    @abstractmethod
    def transform(self) -> List[DataFrame]:
        """Function for applying the main bulk of data transformations.

        Raises:
            NotImplementedError: Raise when child class does not implement this
            method.

        Returns:
            List[DataFrame]: A list of transformed dataframes.
        """
        raise NotImplementedError

    def cache_table(self, df: DataFrame):
        """Cache tables and appends them to list.

        This method caches a dataframe and appends them to a list.
        All dataframes in the list are unpersisted upon run completion.

        Args:
            df (DataFrame): df
        """
        logger.info("Caching dataframe")
        df = df.cache()
        self.cached_tables.append(df)

    def _unpersist_cache(self):
        """Unpersists all cached tables

        This method unpersists all dataframes cached using the
        `cache_table` method. Method called at the end of the run.
        """
        for df in self.cached_tables:
            df.unpersist()
            
    def run(
        self,
        partition_by: Union[List[str], str, None] = None,
        optimize_table: bool = False,
    ) -> None:
        """The entry point that will start the processing of the data for each layer.

        Args:
            partition_by (Union[List[str], str, None], optional): Columns as a list of strings to partition
            the data by. Defaults to None.
            optimize_table (bool, optional): Decide wether to optimise data after write. Defaults to False.

        Raises:
            Exception: When the transformation fails.
            Exception: When tables have failed to save.
        """
        try:
            dfs = self.transform()
        except Exception as error:
            logger.exception("Failed to apply transformation")
            raise error

        for df in dfs:
            full_db_table_name: str = df["table_name"].split(".")
            database_name = full_db_table_name[0]
            table_name = full_db_table_name[1]
            try:
                save(
                    spark=self.spark,
                    df=df["df"],
                    destination_path=self.destination_path,
                    database_name=database_name,
                    table_name=table_name,
                    file_format=self.destination_file_format,
                    overwrite=self.overwrite_table,
                    partition_by=partition_by,
                    optimize_table=optimize_table,
                )
            except Exception as error:
                logger.exception("Failed to save tables")
                raise error
        self._unpersist_cache()