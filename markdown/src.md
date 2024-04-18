<a id="main"></a>

# main

<a id="src"></a>

# src

Contains utility files for various systems in the project.

<a id="src.tasks"></a>

# src.tasks

Tasks

Folder is designed to house the executable notebooks in databricks used by databricks
workflows.

<a id="src.config"></a>

# src.config

Script to initialise logger from yml file

**Attributes**:

- `LOGGER_CONFIG_PATH` _str_ - The relative path to the logging yml file.

<a id="src.utils.spark_utils"></a>

# src.utils.spark_utils

Spark Utils

Contains spark utility functions.

<a id="src.utils.spark_utils.create_abfss_path"></a>

#### create_abfss_path

```python
def create_abfss_path(container: str,
                      storage_account: str = None,
                      path: str = None) -> str
```

Creates path to Azure Data Lake Storage Gen2.

**Arguments**:

- `container` _str_ - Name of storage container.
- `storage_account` _str_ - Storage account name. Defaults to None.
- `path` _str_ - Path to storage account. Defaults to None.

**Returns**:

- `str` - The constructed azure dbfs path.

<a id="src.utils.spark_utils.save"></a>

#### save

```python
def save(spark: SparkSession,
         df: DataFrame,
         destination_path: str,
         database_name: str,
         table_name: str,
         file_format: str,
         overwrite: bool,
         optimize_table: bool = False,
         partition_by: Union[List[str], str, None] = None) -> None
```

Create a database if it doesn't exist. Append a timestamp to the dataframe before saving as a table.
Optional optimization of table.

**Arguments**:

- `spark` _SparkSession_ - Entry point for PySpark application.
- `df` _DataFrame_ - PySpark dataframe.
- `destination_path` _str_ - Location to save dataframe.
- `database_name` _str_ - Name of database.
- `table_name` _str_ - Name of table.
- `file_format` _str_ - Format in which to save table.
- `overwrite` _bool_ - True overwrites current table. False appends.
- `optimize_table` _bool_ - Defaults to False.
- `partition_by` _Union[str, list[str], None]_ - Partitions table based on one or more columns. Defaults to None.

**Examples**:

```python
>>> spark=SparkSession.builder.getOrCreate()
>>> df=Spark.createDataFrame([(0,1,2),(3,4,5)])
>>> save(
        spark=spark,
        df=df,
        destination_path="destination",
        database_name="my_database",
        table_name="my_table",
        file_format="delta",
        overwrite=False,
        optimize_table=False,
        partition_by=["year", "month"]
    )
```

<a id="src.utils"></a>

# src.utils

Folder where code for common utilities for the project lives. The subfolder
preprocessing contains reusable transformation functions for pyspark dataframe transformations.

<a id="src.utils.preprocessing"></a>

# src.utils.preprocessing

Folder containing reusable transformation code for pyspark dataframe transformations.

<a id="src.utils.preprocessing.transformation"></a>

# src.utils.preprocessing.transformation

<a id="src.utils.preprocessing.transformation.ColumnException"></a>

## ColumnException Objects

```python
class ColumnException(Exception)
```

Exception to be thrown whenever a column is invalid

<a id="src.utils.preprocessing.transformation.replace_column_names"></a>

#### replace_column_names

```python
def replace_column_names(df: DataFrame,
                         column_pair: List[Tuple[str, str]]) -> DataFrame
```

Given a list a of column names in the form (A,B) replace the column
A with B in the dataframe.

**Arguments**:

- `df` _DataFrame_ - The dataframe to transform.
- `column_pair` _List[Tuple[str, str]]_ - A pair of string columns in the form (A,B)

**Returns**:

- `DataFrame` - The transformed dataframe.

**Raises**:

- `ColumnException` - Rises Column Exception when column not in dataframe

<a id="src.utils.preprocessing.transformation.create_column_pair_list"></a>

#### create_column_pair_list

```python
def create_column_pair_list(df: DataFrame, col_left: str,
                            col_right: str) -> List[Tuple]
```

Create a list of tuples that map contents in column `col_left` to `col_right` row-wise.

**Arguments**:

- `df` _DataFrame_ - Dataframe to create list of tuples.
- `col_left` _str_ - Column name of the reference column.
- `col_right` _str_ - Column name of the mapped column.

**Returns**:

- `List[Tuple]` - The column pair to return.

**Examples**:

```python
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
| A | B | C |
+---+---+---+
| 1 | 2 | 3 |
+---+---+---+
| 4 | 5 | 6 |
+---+---+---+
>>> create_column_pair_list(df, "B", "C")
[(2,3),(5,6)]
```

**Raises**:

- `ColumnException` - Raises Column Exception when duplicate pair in dataframe.

<a id="src.utils.preprocessing.transformation.clean_column_names"></a>

#### clean_column_names

```python
def clean_column_names(df: DataFrame) -> DataFrame
```

Given a dataframe apply a regex to clean special characters and replace the
spaces with underscores.

**Arguments**:

- `df` _DataFrame_ - The dataframe to be transformed.

**Returns**:

- `DataFrame` - The transformed dataframe.

<a id="src.utils.preprocessing.transformation.regex_replace_column"></a>

#### regex_replace_column

```python
def regex_replace_column(df, original_column: str,
                         regex_replacement_pair: Tuple[str, str]) -> DataFrame
```

Given a dataframe, replace a column value, if it matches a regex
pattern with the new pattern.

**Arguments**:

- `df` _DataFrame_ - A dataframe to transform.
- `original_column` _DataFrame_ - The column that contains the values to replace.
- `regex_replacement_pair` _Tuple[str, str]_ - A tuple pair containing the regex pattern and the value to replace it with.

**Returns**:

- `DataFrame` - The transformed dataframe.

<a id="src.utils.preprocessing.transformation.join_table"></a>

#### join_table

```python
def join_table(left_table: DataFrame,
               right_table: DataFrame,
               join_pair: Tuple[str, str],
               how: str,
               drop_joined_column: bool = True,
               other_drop_columns: Optional[List[str]] = None) -> DataFrame
```

Join two dataframes using tuple pair with the left and right dataframe column
to join on respectively.

**Arguments**:

- `left_table` _DataFrame_ - The left table of the join.
- `right_table` _DataFrame_ - The right table of the join.
- `join_pair` _Tuple[str,str]_ - A pair containing a left column name and right column name respectively.
- `how` _str_ - How the join is conducted.
- `drop_joined_column` _Optional[List[str]]_ - List of columns to drop. Defaults to None.

**Returns**:

- `DataFrame` - The dataframe to be transformed.

<a id="src.utils.preprocessing.transformation.apply_prefix"></a>

#### apply_prefix

```python
def apply_prefix(df: DataFrame, prefix: str)
```

Apply a prefix on to the name of each of the columns seperated by `_`

**Arguments**:

- `df` _DataFrame_ - The pyspark dataframe.
- `prefix` _str_ - The prefix for the columns.

**Returns**:

- `DataFrame` - The transformed dataframe.

<a id="src.utils.pandera_utils"></a>

# src.utils.pandera_utils

Pandera Utils

File containing functions that interact with Pandera library.

<a id="src.utils.pandera_utils.is_continuous_number"></a>

#### is_continuous_number

```python
def is_continuous_number(numbers: List[int],
                         acceptble_max_values: List[int]) -> bool
```

Checks if number is continuous.

**Arguments**:

- `numbers` _int_ - List of values to be checked.
- `acceptable_max_values` _list[int]_ - Acceptable maximum values in the list of numbers.

**Returns**:

True for success, False otherwise.

**Examples**:

```python
>>>is_continuous_number(numbers=[1,2,3,4,5,1,2,3,4,5], acceptable_max_values=[5])
True
>>>is_continuous_number(numbers=[1,2,4,5,1,2,3,4,5], acceptable_max_values=[5])
False
```

<a id="src.utils.pandera_utils.cast_table"></a>

#### cast_table

```python
def cast_table(df: DataFrame,
               pandera_dataframe_model: DataFrameModel) -> DataFrame
```

Takes Pandera schema and applies type to dataframe.

**Arguments**:

- `df` _DataFrame_ - PySpark data frame.
- `pandera_dataframe_model` _DataFrameModel_ - Pandera dataframe schema.

**Returns**:

- `DataFrame` - PySpark data frame with types cast.

<a id="src.utils.file_handler"></a>

# src.utils.file_handler

File Handler

The filehandler contains code for zipping and unzipping files between Azure Storage
container and databricks file system.

**Attributes**:

- `DBFS_FS_PREFIX` _str_ - The databricks file system api prefix.
- `DBFS_LOCAL_ROOT` _str_ - The databricks Ubuntu machine file prefix.

<a id="src.utils.file_handler.Loader"></a>

## Loader Objects

```python
class Loader(yaml.SafeLoader)
```

YAML Loader with `!include` constructor.

<a id="src.utils.file_handler.Loader.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream: IO) -> None
```

Initialise Loader.

<a id="src.utils.file_handler.unzip_file_to_storage_container"></a>

#### unzip_file_to_storage_container

```python
def unzip_file_to_storage_container(dbutils, sa_path: str) -> None
```

Unzips files from in Azure storage container.

**Arguments**:

- `dbutils` _Any_ - utilities package provided by Databricks environment.
- `sa_path` _str_ - storage account path.

**Raises**:

- `Exception` - If files fail to unzip.

<a id="src.utils.file_handler.zip_files_to_storage_account"></a>

#### zip_files_to_storage_account

```python
def zip_files_to_storage_account(dbutils, sa_path: str,
                                 exclusion_list: List[str]) -> None
```

Compresses files into Azure storage account.

**Arguments**:

- `dbutils` _Any_ - utilities package provided by Databricks environment.
- `sa_path` _str_ - storage account path.
- `exclusion_list` _list[str]_ - list of files to exclude from compressing.

**Raises**:

- `Exception` - failed to move file from azure storage to dbfs.
- `Exception` - failed to zip files.

<a id="src.utils.mock_dbutils"></a>

# src.utils.mock_dbutils

<a id="src.pipelines"></a>

# src.pipelines

Pipelines folder

Contains JSON templates for each independent Databricks workflow.

<a id="src.spark_config"></a>

# src.spark_config

<a id="src.spark_config.get_spark_session"></a>

#### get_spark_session

```python
def get_spark_session(name: str) -> SparkSession
```

The session that runs the main spark application.

**Arguments**:

- `name` _str_ - The name of the spark application.

**Returns**:

- `SparkSession` - A spark session configured with delta.

<a id="src.transformations.to_bronze"></a>

# src.transformations.to_bronze

The folder where transformation code for landing to bronze lives. The child folder is typically the source name
with code ins inside it.

<a id="src.transformations.to_gold"></a>

# src.transformations.to_gold

The folder where transformation code for silver to gold lives. The child folder is typically the source name
with code ins inside it.

<a id="src.transformations"></a>

# src.transformations

Folder where code for transformations live for each of the layers.

<a id="dummy_validation_schemas"></a>

# dummy_validation_schemas

Pandera Validation Example

In order to evaluate the data quality we are using the Pandera library. The Library allows you to perform
a number of checks on the data. Pandera works by defining a class where each variable has a type associated with it
each variable is then assigned a Pandera field check.

More details see: https://pandera.readthedocs.io/en/stable/pyspark_sql.html

**Examples**:

```python
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
- `{'DATA'` - defaultdict(<class 'list'>, {'DATAFRAME_CHECK': [{'schema': 'DummySchema', 'column': 'letter_set', 'check': "isin(['A', 'B'])", 'error': "column 'letter_set' with type StringType failed validation isin(['A', 'B'])"}]})}
```

<a id="dummy_validation_schemas.PanderasDataValidationException"></a>

## PanderasDataValidationException Objects

```python
class PanderasDataValidationException(Exception)
```

Raised when validations fail

<a id="dummy_validation_schemas.is_less_than_date"></a>

#### is_less_than_date

```python
@register_check_method()
def is_less_than_date(pandera_pyspark_obj: PanderaProperties) -> bool
```

A custom pandera function that checks if the date is less than than
the current date.

**Arguments**:

- `pandera_pyspark_obj` _PanderaProperties_ - The object produced from a pandera validation.

**Returns**:

- `bool` - True if all the values in a column are less than the current date.

<a id="dummy_validation_schemas.is_greater_than_date"></a>

#### is_greater_than_date

```python
@register_check_method()
def is_greater_than_date(pandera_pyspark_obj: PanderaProperties) -> bool
```

A custom pandera function that checks if the date is greater than than
the current date.

**Arguments**:

- `pandera_pyspark_obj` _PanderaProperties_ - The object produced from a pandera validation.

**Returns**:

- `bool` - True if all the values in a column are greater than the current date.

<a id="dummy_validation_schemas.is_week_continuous"></a>

#### is_week_continuous

```python
@register_check_method()
def is_week_continuous(pyspark_obj) -> bool
```

A custom pandera check to see if the week numbers are continuous.

**Arguments**:

- `pandera_pyspark_obj` _PanderaProperties_ - The object produced from a pandera validation.

**Returns**:

- `bool` - True if all week values are continuous.

<a id="dummy_validation_schemas.DummyAirportSchema"></a>

## DummyAirportSchema Objects

```python
class DummyAirportSchema(DataFrameModel)
```

The Pandera airport schema

<a id="dummy_validation_schemas.DummyCountrySchema"></a>

## DummyCountrySchema Objects

```python
class DummyCountrySchema(DataFrameModel)
```

The Pandera country schema

<a id="dummy_validation_schemas.DummyPersonSchema"></a>

## DummyPersonSchema Objects

```python
class DummyPersonSchema(DataFrameModel)
```

The Pandera person schema

# src.transformations.to_silver

The folder where transformation code for bronze to silver lives. The child folder is typically the source name
with code ins inside it.

<a id="src.transformations.base_transformer"></a>

# src.transformations.base_transformer

<a id="src.transformations.base_transformer.BaseTransformer"></a>

## BaseTransformer Objects

```python
class BaseTransformer(metaclass=ABCMeta)
```

Abstract class that contains functionality that can be used commonly between
the data pipeline layers.

New class objects can be initialised by inheriting from the parent class. After
inheriting, the child class should contain the transform() method.

**Arguments**:

- `metaclass` _class, ABCMeta_ - Allows for a mix of concrete and abstract classes. Defaults to ABCMeta.

<a id="src.transformations.base_transformer.BaseTransformer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(dbutils,
             task_name: str,
             destination_path: str,
             overwrite_table=False,
             destination_file_format: str = "DELTA",
             spark: Optional[SparkSession] = None) -> None
```

Constructor for the BaseTransformer

**Arguments**:

- `dbutils` _dbutils_ - The databricks environment utility package.
- `task_name` _str_ - The name the spark application will take for its duration.
- `destination_path` _str_ - The destination path the where the database will live.
- `database_name` _str_ - The name of the database.
- `overwrite_table` _bool, optional_ - Set whether or not to overwrite table. Defaults to False.
- `destination_file_format` _str, optional_ - The file format the dataframe should be saved as. Defaults to "DELTA".
- `spark` _Optional[SparkSession], optional_ - The main spark instance. Defaults to None.

**Attributes**:

- `cached_tables` _List[DataFrame]_ - The list of dataframes that have been cached.

<a id="src.transformations.base_transformer.BaseTransformer.transform"></a>

#### transform

```python
@abstractmethod
def transform() -> List[DataFrame]
```

Function for applying the main bulk of data transformations.

**Raises**:

- `NotImplementedError` - Raise when child class does not implement this
  method.

**Returns**:

- `List[DataFrame]` - A list of transformed dataframes.

<a id="src.transformations.base_transformer.BaseTransformer.cache_table"></a>

#### cache_table

```python
def cache_table(df: DataFrame)
```

Cache tables and appends them to list.

This method caches a dataframe and appends them to a list.
All dataframes in the list are unpersisted upon run completion.

**Arguments**:

- `df` _DataFrame_ - df

<a id="src.transformations.base_transformer.BaseTransformer.run"></a>

#### run

```python
def run(partition_by: Union[List[str], str, None] = None,
        optimize_table: bool = False) -> None
```

The entry point that will start the processing of the data for each layer.

**Arguments**:

- `partition_by` _Union[List[str], str, None], optional_ - Columns as a list of strings to partition
  the data by. Defaults to None.
- `optimize_table` _bool, optional_ - Decide wether to optimise data after write. Defaults to False.

**Raises**:

- `Exception` - When the transformation fails.
- `Exception` - When tables have failed to save.

<a id="src.transformations.to_landing"></a>

# src.transformations.to_landing

The folder where code specific for retrieving and placing files in a digestible format to landing lives. The child folder is typically the source name
with code inside it.
