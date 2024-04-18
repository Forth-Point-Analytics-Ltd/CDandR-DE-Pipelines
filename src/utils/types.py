from typing import TypedDict
from pyspark.sql import DataFrame


class DataFrameProperties(TypedDict):
    table_name: str
    df: DataFrame


class PanderaProperties(TypedDict):
    column_name: str
    dataframe: DataFrame
