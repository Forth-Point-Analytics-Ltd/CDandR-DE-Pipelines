"""Pandera Validation Example

In order to evaluate the data quality we are using the Pandera library. The Library allows you to perform
a number of checks on the data. Pandera works by defining a class where each variable has a type associated with it
each variable is then assigned a Pandera field check.

More details see: https://pandera.readthedocs.io/en/stable/pyspark_sql.html

Examples:
    >>> class DummySchema(DataFrameModel):
    ...     id: IntegerType = pa.Field(nullable=True, ge=0)
    ...     letter_set: StringType = pa.Field(unique=False, isin=["A", "B"])
    ... 
    >>> df = spark.createDataFrame(
    ...     data=[(0,"A"),(1,"B"),(2,"A")],
    ...     schema=st.StructType(
    ...             [
    ...                     st.StructField("id", st.IntegerType()),
    ...                     st.StructField("letter_set", st.StringType()),
    ...             ]
    ...     )
    ... )
    >>> result = DummySchema.validate(df)
    >>> result.pandera.errors
    {}
    >>> df = spark.createDataFrame(
    ...     data=[(0,"A"),(1,"B"),(2,"C")],
    ...     schema=st.StructType(
    ...             [
    ...                     st.StructField("id", st.IntegerType()),
    ...                     st.StructField("letter_set", st.StringType()),
    ...             ]
    ...     )
    ... )
    >>> result = DummySchema.validate(df)
    >>> result.pandera.errors
    {'DATA': defaultdict(<class 'list'>, {'DATAFRAME_CHECK': [{'schema': 'DummySchema', 'column': 'letter_set', 'check': "isin(['A', 'B'])", 'error': "column 'letter_set' with type StringType failed validation isin(['A', 'B'])"}]})}
    
"""
from pyspark.sql.types import IntegerType, StringType, DateType, DecimalType
from pyspark.sql.functions import col, lit, collect_list, max as spark_max
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel
from pandera.extensions import register_check_method
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.utils.pandera_utils import is_continuous_number
from src.utils.types import PanderaProperties


class PanderasDataValidationException(Exception):
    """
    Raised when validations fail
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


@register_check_method()
def is_less_than_date(pandera_pyspark_obj: PanderaProperties) -> bool:
    """A custom pandera function that checks if the date is less than than
    the current date.

    Args:
        pandera_pyspark_obj (PanderaProperties): The object produced from a pandera validation.

    Returns:
        bool: True if all the values in a column are less than the current date.
    """
    cond = col(pandera_pyspark_obj.column_name) <= lit(datetime.now())
    return pandera_pyspark_obj.dataframe.filter(~cond).limit(1).count() == 0


@register_check_method()
def is_greater_than_date(pandera_pyspark_obj: PanderaProperties) -> bool:
    """A custom pandera function that checks if the date is greater than than
    the current date.

    Args:
        pandera_pyspark_obj (PanderaProperties): The object produced from a pandera validation.

    Returns:
        bool: True if all the values in a column are greater than the current date.
    """
    max_date = pandera_pyspark_obj.dataframe.select(
        spark_max(col("date")).alias("max_date")
    ).head()["max_date"]
    min_date = max_date - relativedelta(years=5)
    cond = col(pandera_pyspark_obj.column_name) >= lit(min_date)
    return pandera_pyspark_obj.dataframe.filter(~cond).limit(1).count() == 0


@register_check_method()
def is_week_continuous(pyspark_obj) -> bool:
    """A custom pandera check to see if the week numbers are continuous.

    Args:
        pandera_pyspark_obj (PanderaProperties): The object produced from a pandera validation.

    Returns:
        bool: True if all week values are continuous.
    """
    week_list = pyspark_obj.dataframe.select(collect_list("week")).first()[-1]
    return is_continuous_number(
        week_list, acceptble_max_values=[49, 50, 51, 52, 53, 54]
    )


class DummyAirportSchema(DataFrameModel):
    """The Pandera airport schema"""

    id: IntegerType = pa.Field(nullable=True, ge=0)
    airport_code: StringType = pa.Field(unique=False)
    airport_latitude: DecimalType = pa.Field(nullable=True, ge=0)
    airport_longitude: DecimalType = pa.Field(nullable=True, ge=0)
    airport_name: StringType = pa.Field(unique=False)
    airport_country_code: StringType = pa.Field(unique=False)


class DummyCountrySchema(DataFrameModel):
    """The Pandera country schema"""

    country_id: IntegerType = pa.Field(nullable=True, ge=0)
    country_country: StringType = pa.Field(unique=True)
    country_country_code: StringType = pa.Field(unique=True)


class DummyPersonSchema(DataFrameModel):
    """The Pandera person schema"""

    person_id: IntegerType = pa.Field(unique=True, ge=0)
    person_first_name: StringType = pa.Field(unique=False)
    person_last_name: StringType = pa.Field(nullable=True)
    person_email: StringType = pa.Field(unique=True)
    person_gender: StringType = pa.Field(nullable=True)
    person_airport_code: StringType = pa.Field()


SILVER_SCHEMA_MAPPING = {
    "person": DummyPersonSchema,
    "airport": DummyAirportSchema,
    "country": DummyCountrySchema,
}
