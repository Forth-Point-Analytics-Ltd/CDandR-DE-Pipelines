from typing import List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, collect_list
import re


class ColumnException(Exception):
    """Exception to be thrown whenever a column is invalid"""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def replace_column_names(
    df: DataFrame, column_pair: List[Tuple[str, str]]
) -> DataFrame:
    """Given a list a of column names in the form (A,B) replace the column
    A with B in the dataframe.

    Args:
        df (DataFrame): The dataframe to transform.
        column_pair (List[Tuple[str, str]]): A pair of string columns in the form (A,B)

    Returns:
        DataFrame: The transformed dataframe.

    Raises:
        ColumnException: Rises Column Exception when column not in dataframe
    """
    output_df = df
    for original_name, replace_name in column_pair:
        if original_name not in df.columns:
            raise ColumnException(f"Column {original_name} not in DataFrame")
        output_df = output_df.withColumnRenamed(original_name, replace_name)
    return output_df


def create_column_pair_list(
    df: DataFrame, col_left: str, col_right: str
) -> List[Tuple]:
    """Create a list of tuples that map contents in column `col_left` to `col_right` row-wise.

    Args:
        df (DataFrame): Dataframe to create list of tuples.
        col_left (str): Column name of the reference column.
        col_right (str): Column name of the mapped column.

    Returns:
        List[Tuple]: The column pair to return.
    Examples:
        >>> df = spark.createDataFrame(
                [(1, 2, 3), (4, 5, 6)],
                schema=st.StructType(
                    [
                        st.StructField("A", st.StringType()),
                        st.StructField("B", st.StringType()),
                        st.StructField("C", st.StringType()),
                    ]
                ),
            )
        >>> df.show()
        +---+---+---+
        |  A|  B|  C|
        +---+---+---+
        |  1|  2|  3|
        +---+---+---+
        |  4|  5|  6|
        +---+---+---+
        >>> create_column_pair_list(df, "B", "C")
        [(2,3),(5,6)]
    Raises:
        ColumnException: Raises Column Exception when duplicate pair in dataframe.
    """
    left = df.select(collect_list(col_left)).first()[-1]
    right = df.select(collect_list(col_right)).first()[-1]
    if len(set(left)) == len(set(right)):
        column_pair = list(zip(left, right))
    else:
        raise ColumnException("Duplicate value in pair")
    return column_pair


def clean_column_names(df: DataFrame) -> DataFrame:
    """Given a dataframe apply a regex to clean special characters and replace the
    spaces with underscores.

    Args:
        df (DataFrame): The dataframe to be transformed.
    Returns:
        DataFrame: The transformed dataframe.
    """
    output_df = df
    for original_column_name in output_df.columns:
        new_column_word_list = [
            word
            for word in (re.split(r"[^A-Za-z0-9]", original_column_name))
            if word != ""
        ]
        new_column_name = "_".join(new_column_word_list)
        output_df = output_df.withColumnRenamed(
            original_column_name, new_column_name.lower()
        )
    return output_df


def regex_replace_column(
    df, original_column: str, regex_replacement_pair: Tuple[str, str]
) -> DataFrame:
    """Given a dataframe, replace a column value, if it matches a regex
    pattern with the new pattern.

    Args:
        df (DataFrame): A dataframe to transform.
        original_column (DataFrame): The column that contains the values to replace.
        regex_replacement_pair (Tuple[str, str]): A tuple pair containing the regex pattern and the value to replace it with.

    Returns:
        DataFrame: The transformed dataframe.
    """
    df_with_regex_replace = df.withColumn(
        original_column,
        regexp_replace(original_column, *regex_replacement_pair),
    )
    return df_with_regex_replace


def join_table(
    left_table: DataFrame,
    right_table: DataFrame,
    join_pair: Tuple[str, str],
    how: str,
    drop_joined_column: bool = True,
    other_drop_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Join two dataframes using  tuple pair with the left and right dataframe column
    to join on respectively.

    Args:
        left_table (DataFrame): The left table of the join.
        right_table (DataFrame): The right table of the join.
        join_pair (Tuple[str,str]): A pair containing a left column name and right column name respectively.
        how (str): How the join is conducted.
        drop_joined_column (Optional[List[str]]): List of columns to drop. Defaults to None.

    Returns:
        DataFrame: The dataframe to be transformed.
    """
    left_table = left_table.withColumnRenamed(
        join_pair[0], left_name := f"left_{join_pair[0]}"
    )
    right_table = right_table.withColumnRenamed(
        join_pair[1], right_name := f"right_{join_pair[1]}"
    )
    joined_df = left_table.join(
        right_table,
        on=(left_table[left_name] == right_table[right_name]),
        how=how,
    )

    drop_columns = [] if other_drop_columns is None else other_drop_columns
    if drop_joined_column:
        drop_columns.extend([left_name, right_name])
    else:
        joined_df = joined_df.withColumnRenamed(left_name, join_pair[0])
        joined_df = joined_df.withColumnRenamed(right_name, join_pair[1])
    joined_df = joined_df.drop(*drop_columns) if drop_columns else joined_df

    return joined_df


def apply_prefix(df: DataFrame, prefix: str):
    """Apply a prefix on to the name of each of the columns seperated by `_`

    Args:
        df (DataFrame): The pyspark dataframe.
        prefix (str): The prefix for the columns.

    Returns:
        DataFrame: The transformed dataframe.
    """
    for column in df.columns:
        df = df.withColumnRenamed(column, f"{prefix}_{column}")
    return df
