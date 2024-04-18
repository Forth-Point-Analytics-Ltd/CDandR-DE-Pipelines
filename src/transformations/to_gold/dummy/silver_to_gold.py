from typing import List, Optional
from pyspark.sql import SparkSession
from src.transformations.base_transformer import BaseTransformer
from src.utils.types import DataFrameProperties
from src.utils.preprocessing.transformation import join_table


class SilverToGold(BaseTransformer):
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

    def _dummy_person_airport_location_transformation(
        self, dest_table: str
    ) -> DataFrameProperties:
        dest_table_mapping = self.table_transform_config[dest_table]
        df = self.spark.table(
            f"{self.src_database}.{dest_table_mapping['src_table']}"
        )
        for table in dest_table_mapping["join_tables"]:
            join_pair = (table["column_pairs"][0], table["column_pairs"][1])
            df = join_table(
                left_table=df,
                right_table=self.spark.table(
                    f"{self.src_database}.{table['table']}"
                ),
                join_pair=join_pair,
                how="left",
                other_drop_columns=["processing_ts"],
            )
        self.cache_table(df)

        return {"table_name": f"{self.dst_database}.{dest_table}", "df": df}

    def transform(self) -> List[DataFrameProperties]:
        dfs = []
        for dest_table in self.table_transform_config:
            dest_table_config = self.table_transform_config[dest_table]
            transform_func = getattr(
                self, dest_table_config["transformation"], dest_table
            )
            df = transform_func(dest_table)
            dfs.append(df)
        return dfs

    def run(self) -> None:
        super().run()
