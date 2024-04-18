from src.utils.preprocessing.transformation import (
    replace_column_names,
    regex_replace_column,
    join_table,
    clean_column_names,
    apply_prefix,
    create_column_pair_list,
)
from src.spark_config import get_spark_session
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from pytest import raises, fixture


@fixture
def setup_spark_session():
    return get_spark_session("test_replace_column")


@fixture
def setup_single_dataframe(setup_spark_session):
    spark = setup_spark_session
    df = spark.createDataFrame(
        [(1, 2, 3)],
        schema=st.StructType(
            [
                st.StructField("A", st.StringType()),
                st.StructField("B", st.StringType()),
                st.StructField("C", st.StringType()),
            ]
        ),
    )
    return df


@fixture
def setup_multiple_dataframe(setup_spark_session):
    spark: SparkSession = setup_spark_session
    df = spark.createDataFrame(
        [(1, "2", "3"), ("_4", "5", "6")],
        schema=st.StructType(
            [
                st.StructField("A", st.StringType()),
                st.StructField("B", st.StringType()),
                st.StructField("C", st.StringType()),
            ]
        ),
    )

    other_df = spark.createDataFrame(
        [(1, "2", "3"), ("_4", "5", "6")],
        schema=st.StructType(
            [
                st.StructField("D", st.StringType()),
                st.StructField("E", st.StringType()),
                st.StructField("F", st.StringType()),
            ]
        ),
    )
    return df, other_df


def test_replace_column_names_successful(setup_single_dataframe):
    df = setup_single_dataframe

    df_to_compare = replace_column_names(df, [("A", "D"), ("B", "E")])

    assert df_to_compare.columns == ["D", "E", "C"]


def test_unsuccessful_column_replacement_column_not_exists(
    setup_single_dataframe,
):
    df = setup_single_dataframe

    with raises(Exception) as error_info:
        replace_column_names(df, [("G", "D"), ("B", "E")])
    assert error_info.value.args[0] == "Column G not in DataFrame"


def test_successful_regex_replace_column():
    spark = get_spark_session("test_replace_column")
    df = spark.createDataFrame(
        [("_1", "2", "3"), ("_4", "5", "6")],
        schema=st.StructType(
            [
                st.StructField("A", st.StringType()),
                st.StructField("B", st.StringType()),
                st.StructField("C", st.StringType()),
            ]
        ),
    )
    df_to_compare = regex_replace_column(df, "A", ("_", ""))
    assert df_to_compare.select(sf.collect_list("A")).first()[0] == ["1", "4"]


def test_unsuccessful_regex_replace_column_not_exists():
    spark = get_spark_session("test_replace_column")
    df = spark.createDataFrame(
        [(1, "2", "3"), ("_4", "5", "6")],
        schema=st.StructType(
            [
                st.StructField("A", st.StringType()),
                st.StructField("B", st.StringType()),
                st.StructField("C", st.StringType()),
            ]
        ),
    )

    with raises(Exception) as error_info:
        regex_replace_column(df, "E", ("_", ""))
    assert "cannot resolve 'E'" in error_info.value.args[0]


def test_successful_join_table(setup_multiple_dataframe):
    df, other_df = setup_multiple_dataframe

    joined_1_df = join_table(
        df, other_df, ("A", "D"), how="left", drop_joined_column=True
    )

    assert joined_1_df.columns == ["B", "C", "E", "F"]


def test_successful_join_table_without_dropping_column(
    setup_multiple_dataframe,
):
    df, other_df = setup_multiple_dataframe

    joined_2_df = join_table(
        df, other_df, ("A", "D"), how="left", drop_joined_column=False
    )

    assert joined_2_df.columns == ["A", "B", "C", "D", "E", "F"]


def test_successful_join_table_drop_non_joined_columns(
    setup_multiple_dataframe,
):
    df, other_df = setup_multiple_dataframe

    joined_2_df = join_table(
        df,
        other_df,
        ("A", "D"),
        how="left",
        drop_joined_column=False,
        other_drop_columns=["C", "F"],
    )

    assert joined_2_df.columns == ["A", "B", "D", "E"]


def test_successful_join_table_drop_joined_and_other_columns(
    setup_multiple_dataframe,
):
    df, other_df = setup_multiple_dataframe
    joined_2_df = join_table(
        df,
        other_df,
        ("A", "D"),
        how="left",
        drop_joined_column=True,
        other_drop_columns=["C", "F"],
    )

    assert joined_2_df.columns == ["B", "E"]


def test_unsuccessful_join_table_when_column_does_not_exist(
    setup_multiple_dataframe,
):
    df, other_df = setup_multiple_dataframe

    with raises(Exception) as error_info:
        join_table(
            df, other_df, ("A", "A"), how="left", drop_joined_column=True
        )
    assert "Cannot resolve" in error_info.value.args[0]


def test_successful_clean_column_names():
    spark = get_spark_session("test_replace_column")
    df = spark.createDataFrame(
        [(1, "2", "3"), ("_4", "5", "6")],
        schema=st.StructType(
            [
                st.StructField("A*.,@D", st.StringType()),
                st.StructField("B!'#^", st.StringType()),
                st.StructField("C+=", st.StringType()),
            ]
        ),
    )

    cleaned_df = clean_column_names(df)
    assert cleaned_df.columns == ["a_d", "b", "c"]


def test_apply_prefix(setup_single_dataframe):
    df = setup_single_dataframe
    expected = ["pre_A", "pre_B", "pre_C"]
    actual = apply_prefix(df, prefix="pre").columns
    assert expected == actual


def test_create_column_pair_list_successful(setup_single_dataframe):
    df = setup_single_dataframe
    expected = [("1", "2")]
    actual = create_column_pair_list(df, "A", "B")
    assert expected == actual


def test_create_column_pair_list_unsuccessful(setup_single_dataframe):
    df = setup_single_dataframe
    with raises(Exception) as error_info:
        create_column_pair_list(df, "E", "F")
    assert "cannot resolve 'E' given input columns" in error_info.value.args[0]


def test_create_column_pair_list_duplicate_value_unsuccessful(
    setup_single_dataframe, setup_spark_session
):
    df: DataFrame = setup_single_dataframe
    spark: SparkSession = setup_spark_session
    cols = df.columns
    new_row = spark.createDataFrame([("1", "4", "5")], cols)
    df = df.union(new_row)
    with raises(Exception) as error_info:
        create_column_pair_list(df, "A", "B")
    assert "Duplicate value in pair" in error_info.value.args[0]
