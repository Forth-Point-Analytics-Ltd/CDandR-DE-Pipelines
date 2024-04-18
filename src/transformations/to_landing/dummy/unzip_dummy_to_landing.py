from src.utils.file_handler import unzip_file_to_storage_container
from src.utils.spark_utils import create_abfss_path


def dummy_to_landing(dbutils, env_prefix: str) -> None:
    unzip_file_to_storage_container(
        dbutils, create_abfss_path(f"{env_prefix}-landing", path="dummy")
    )
