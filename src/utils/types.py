from datetime import datetime
from decimal import Decimal
from typing import NamedTuple, TypedDict
from pyspark.sql import DataFrame


class DataFrameProperties(TypedDict):
    table_name: str
    df: DataFrame


class PanderaProperties(TypedDict):
    column_name: str
    dataframe: DataFrame


class ProductInfo(NamedTuple):
    timestamp: datetime
    product_name: str
    brand_name: str
    url: str
    product_price_now: Decimal
    product_quantity: int
    product_size: int
    product_size_unit: str
    product_price_per: Decimal
    product_price_per_unit: str


class Quantity(NamedTuple):
    multiplier: int
    quantity: int
    unit: str


class PricePer(NamedTuple):
    price: Decimal
    unit: str
