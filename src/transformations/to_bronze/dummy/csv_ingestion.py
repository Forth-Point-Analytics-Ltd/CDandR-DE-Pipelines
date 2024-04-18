from typing import List, Optional, Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from src.transformations.base_transformer import BaseTransformer
from src.utils.types import DataFrameProperties
from src.utils.file_handler import zip_files_to_storage_account
from src.utils.preprocessing.transformation import clean_column_names

from src.config import logger


class CSVBulkIngestionToBronze(BaseTransformer):
    def __init__(
        self,
        dbutils,
        src_path: str,
        archive_path: str,
        destination_path: str,
        task_name: str,
        table_transform_config: dict,
        dst_database: str,
        sep: str = ",",
        overwrite_table=False,
        destination_file_format: str = "DELTA",
        spark: Optional[SparkSession] = None,
    ) -> None:
        super().__init__(
            dbutils,
            task_name,
            destination_path,
            overwrite_table,
            destination_file_format,
            spark,
        )
        self.src_path = src_path
        self.archive_path = archive_path
        self.sep = sep
        self.dst_database = dst_database
        self.table_transform_config = table_transform_config

    def load(
        self, dbutils, file_path: str
    ) -> Tuple[List[DataFrameProperties], Dict[str, DataFrameProperties]]:
        dfs = []

        files = {
            file_obj.name: file_obj.path
            for file_obj in dbutils.fs.ls(file_path)
        }

        for dest_table in self.table_transform_config:
            file_name = (
                f"{self.table_transform_config[dest_table]['file_name']}.csv"
            )
            logger.info(f"Loading {files[file_name]}")
            if file_name in files.keys():
                df = self.spark.read.format("csv").load(
                    files[file_name], sep=self.sep, header=True
                )
                logger.info(f"Successfully loaded {files[file_name]}")
                dfs.append(
                    {
                        "table_name": f"{self.dst_database}.{dest_table}",
                        "df": df,
                    }
                )
        return dfs

    def transform(self) -> List[DataFrame]:
        dfs = self.load(self.dbutils, self.src_path)
        output_dfs = []
        for df_dict in dfs:
            df = df_dict["df"]
            logger.info(f"Cleaning {df_dict['table_name']} columns")
            df_dict["df"] = clean_column_names(df)
            output_dfs.append(df_dict)
        return output_dfs

    def move_data_to_archive(self) -> None:
        zip_files_to_storage_account(self.dbutils, self.src_path, ["archive/"])
        for file in self.dbutils.fs.ls(self.src_path):
            if file.name != "archive/":
                logger.info(
                    f"Moving {file.name} to {self.archive_path}{file.name}"
                )
                self.dbutils.fs.mv(
                    file.path, f"{self.archive_path}{file.name}"
                )

    def run(self) -> None:
        super().run()
        self.move_data_to_archive()
