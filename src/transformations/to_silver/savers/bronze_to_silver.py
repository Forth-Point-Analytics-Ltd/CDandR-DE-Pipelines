from typing import List
from pyspark.sql import DataFrame, SparkSession
from src.config import logger
from src.transformations.base_transformer import BaseTransformer
from src.utils.types import DataFrameProperties
from src.utils.preprocessing.transformation import apply_prefix
from pyspark.sql.functions import (
    col,
    regexp_extract,
    when,
    lit,
    lower,
    regexp_replace,
)
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    TimestampType,
    StringType,
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
        self.table_transform_config = table_transform_config
        self.src_database = src_database
        self.dst_database = dst_database
        self.product_pattern = r"(?i)(\b\d+(?:\s*[x×]\s*\d+)?\.?\d*)\s?(ml|cl|g|kg|l|s|pk|litre|pack|pads|'s)\b"
        self.product_single_multiplier_pattern = r"[xX]\s*(\d+)"
        self.product_multiplier_left_pattern = r"(\d+) x"
        self.product_multiplier_right_pattern = r"x (\d+)"

    def savers_transformation(self, df: DataFrame) -> DataFrame:
        product_name_split_df = (
            df.withColumn(
                "q1",
                regexp_extract(col("product_name"), self.product_pattern, 0),
            )
            .withColumn(
                "product_size",
                regexp_extract(col("product_name"), self.product_pattern, 1),
            )
            .withColumn(
                "product_size_unit",
                regexp_extract(col("product_name"), self.product_pattern, 2),
            )
            .withColumn(
                "q1",
                when(
                    col("q1") == "",
                    regexp_extract(
                        col("product_name"),
                        self.product_single_multiplier_pattern,
                        0,
                    ),
                ).otherwise(col("q1")),
            )
            .withColumn(
                "product_size",
                when(
                    col("product_size") == "",
                    regexp_extract(
                        col("product_name"),
                        self.product_single_multiplier_pattern,
                        1,
                    ),
                ).otherwise(col("product_size")),
            )
            .withColumn(
                "product_quantity",
                when(
                    lower(col("product_size")).contains(lit("x")),
                    regexp_extract(
                        col("product_size"),
                        self.product_multiplier_left_pattern,
                        1,
                    ),
                ).otherwise(lit("1")),
            )
            .withColumn(
                "product_size",
                when(
                    lower(col("product_size")).contains(lit("x")),
                    regexp_extract(
                        col("product_size"),
                        self.product_multiplier_right_pattern,
                        1,
                    ),
                ).otherwise(col("product_size")),
            )
            .withColumn(
                "product_size",
                when(col("product_size") == "", lit("1")).otherwise(
                    col("product_size")
                ),
            )
            .withColumn(
                "product_size_unit",
                when(col("product_size_unit") == "", lit("ea")).otherwise(
                    col("product_size_unit")
                ),
            )
        )
        clean_product_name_split_df = (
            product_name_split_df.withColumn(
                "product_size",
                when(
                    (lower(col("product_size_unit")).contains("litre"))
                    | (lower(col("product_size_unit")) == ("l")),
                    col("product_size").cast(DoubleType())
                    * lit(1000).cast(IntegerType()),
                ).otherwise(col("product_size")),
            )
            .withColumn("product_size_unit", lower(col("product_size_unit")))
            .withColumn(
                "product_size_unit",
                when(
                    (col("product_size_unit").contains("litre"))
                    | (col("product_size_unit") == ("l")),
                    lit("ml"),
                )
                .when(
                    (col("product_size_unit") == "s")
                    | (col("product_size_unit") == "S")
                    | (col("product_size_unit") == "'s")
                    | (col("product_size_unit") == "pk")
                    | (col("product_size_unit") == "pack")
                    | (col("product_size_unit") == "pads"),
                    lit("ea"),
                )
                .otherwise(col("product_size_unit")),
            )
            .withColumn(
                "product_price_per_unit",
                when((col("product_size_unit") == "g"), lit("100g"))
                .when((col("product_size_unit") == "ml"), lit("100ml"))
                .when(col("product_size_unit") == "ea", lit("ea"))
                .otherwise(lit(None)),
            )
            .withColumn(
                "product_price_per",
                when(
                    (col("product_size_unit") == "g")
                    | (col("product_size_unit") == "ml"),
                    (
                        col("product_price_now").cast(DoubleType())
                        / col("product_size").cast(DoubleType())
                    )
                    * (
                        lit(100) / (col("product_quantity").cast(DoubleType()))
                    ),
                )
                .when(
                    col("product_size_unit") == "ea",
                    (
                        col("product_price_now").cast(DoubleType())
                        / col("product_size").cast(DoubleType())
                    )
                    * (lit(1) / (col("product_quantity").cast(DoubleType()))),
                )
                .otherwise(lit(None)),
            )
        ).select(
            regexp_replace(col("product_name"), "&amp;", "&")
            .cast(StringType())
            .alias("product_name"),
            col("brand_name").cast(StringType()),
            col("url").cast(StringType()),
            col("product_price_now").cast(DecimalType(scale=2)),
            col("product_quantity").cast(IntegerType()),
            col("product_size").cast(IntegerType()),
            col("product_size_unit").cast(IntegerType()),
            col("product_price_per").cast(DecimalType(scale=2)),
            col("product_price_per_unit").cast(StringType()),
        )

        return clean_product_name_split_df

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
