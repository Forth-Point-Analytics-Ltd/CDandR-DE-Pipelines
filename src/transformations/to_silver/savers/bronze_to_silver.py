from typing import List
from pyspark.sql import DataFrame, SparkSession
from src.config import logger
from src.transformations.base_transformer import BaseTransformer
from src.utils.types import DataFrameProperties
from src.utils.preprocessing.transformation import apply_prefix


class BronzeToSilver(BaseTransformer):
    def __init__(
        self,
        dbutils,
        spark: SparkSession,
        task_name: str,
        destination_path: str,
        table_transform_config: dict,
        src_database: str,
        dst_database: str,
        overwrite_table=False,
        destination_file_format: str = "DELTA",
    ) -> None:
        super().__init__(
            dbutils,
            task_name,
            destination_path,
            overwrite_table,
            destination_file_format,
            spark,
        )
        self.table_transform_config = table_transform_config
        self.src_database = src_database
        self.dst_database = dst_database

    def savers_transformation(self, df: DataFrame) -> DataFrame:
        return df

    def transform(self) -> List[DataFrameProperties]:
        dfs = []
        for table_dest_name in self.table_transform_config:
            transformation_config = self.table_transform_config[
                table_dest_name
            ]
            df = self.spark.table(
                f"{self.src_database}.{transformation_config['src_name']}"
            )
            logger.info(
                f"Applying transformation '{transformation_config['transformation']}' to {transformation_config['src_name']}"  # noqa
            )
            transform_func = getattr(
                self, transformation_config["transformation"], df
            )

            df = transform_func(df)

            if "prefix" in transformation_config:
                logger.info(
                    f"Applying prefix '{transformation_config['prefix']}' to {transformation_config['src_name']}"  # noqa
                )
                df = apply_prefix(df, transformation_config["prefix"])

            dfs.append(
                {
                    "table_name": f"{self.dst_database}.{table_dest_name}",
                    "df": df,
                }
            )
        return dfs
