import json
from typing import List, Optional
from pandera.pyspark import DataFrameModel
from pyspark.sql import DataFrame, SparkSession
from src.config import logger
from src.transformations.base_transformer import BaseTransformer
from src.utils.types import DataFrameProperties
from src.utils.preprocessing.transformation import apply_prefix
from src.utils.pandera_utils import cast_table

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import (
    col,
    when,
    monotonically_increasing_id,
    lit,
    row_number,
)
from pyspark.sql import Window


from src.transformations.to_silver.dummy.dummy_validation_schemas import (
    SILVER_SCHEMA_MAPPING,
    PanderasDataValidationException,
)


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
        self.pandera_schemas: dict[str, DataFrameModel] = SILVER_SCHEMA_MAPPING
        self.table_transform_config = table_transform_config
        self.src_database = src_database
        self.dst_database = dst_database

    def validate(self, dfs: List[DataFrameProperties]):
        validation_errors = False
        for df in dfs:
            df_validation_output = None
            df_validation_output = self.pandera_schemas[
                df["table_name"]
            ].validate(df["df"])
            df_errors = df_validation_output.pandera.errors

            if df_errors:
                validation_error = {
                    f"{df['table_name']} Validation Errors": dict(df_errors)
                }
                logger.error(json.dumps(validation_error, indent=4))
                validation_errors = True
            else:
                logger.info(f"{df['table_name']} successfully validated")
        if validation_errors:
            raise PanderasDataValidationException

    def dummy_airport_transformation(self, df: DataFrame) -> DataFrame:
        df = (
            df.withColumn(
                "airport_latitude",
                when(
                    col("airport_latitude").isNull(), lit(0).cast(DecimalType())
                ).otherwise(col("airport_latitude")),
            )
            .withColumn(
                "airport_longitude",
                when(
                    col("airport_longitude").isNull(), lit(0).cast(DecimalType())
                ).otherwise(col("airport_longitude")),
            )
            .drop("processing_ts")
        )
        return df

    def dummy_country_transformation(self, df: DataFrame) -> DataFrame:
        window = Window.orderBy(col("m_unique_id")).partitionBy("partition")
        df = df.select(col("country"), col("country_code")).distinct()
        df = df.select(
            lit(monotonically_increasing_id()).alias("m_unique_id"),
            col("country"),
            col("country_code"),
            lit("partition").alias("partition"),
        )
        df = df.select(
            lit(row_number().over(window)).alias("id"),
            col("country"),
            col("country_code"),
        )
        return df

    def dummy_person_transformation(self, df: DataFrame) -> DataFrame:
        return df.drop("processing_ts")

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
            logger.info(
                f"casting each of the columns in {transformation_config['src_name']}"
            )
            df = cast_table(df, self.pandera_schemas[table_dest_name])

            dfs.append(
                {
                    "table_name": f"{self.dst_database}.{table_dest_name}",
                    "df": df,
                }
            )
        # self.validate(dfs)
        return dfs
