"""Pandera Utils

File containing functions that interact with Pandera library.
"""

from typing import List
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pandera.pyspark import DataFrameModel


def is_continuous_number(
    numbers: List[int], acceptble_max_values: List[int]
) -> bool:
    """Checks if number is continuous.

    Args:
        numbers (int): List of values to be checked.
        acceptable_max_values (list[int]): Acceptable maximum values in the list of numbers.

    Returns:
        True for success, False otherwise.

        Examples:
            >>> is_continuous_number(numbers=[1,2,3,4,5,1,2,3,4,5], acceptable_max_values=[5])
            True
            >>> is_continuous_number(numbers=[1,2,4,5,1,2,3,4,5], acceptable_max_values=[5])
            False
    """
    max_indexes = len(numbers)
    validated = False
    for index in range(0, max_indexes):
        prev = None if index == 0 else index - 1
        current = index
        next = None if (current == (max_indexes - 1)) else index + 1
        if prev and next:
            if (numbers[next] - numbers[current]) != 1:
                if (
                    numbers[next] != 1
                    or numbers[current] not in acceptble_max_values
                ):
                    validated = False
        validated = True
    return validated


def cast_table(
    df: DataFrame, pandera_dataframe_model: DataFrameModel
) -> DataFrame:
    """Takes Pandera schema and applies type to dataframe.

    Args:
        df (DataFrame): PySpark data frame.
        pandera_dataframe_model (DataFrameModel): Pandera dataframe schema.

    Returns:
        DataFrame: PySpark data frame with types cast.
    """
    schema = pandera_dataframe_model.to_schema()
    return df.select(
        *[
            col(schema_col.name).cast(schema_col.dtype.type)
            for schema_col in schema.columns.values()
        ]
    )
