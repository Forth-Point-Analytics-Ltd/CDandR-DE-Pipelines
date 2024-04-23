"""File Handler

The filehandler contains code for zipping and unzipping files between Azure Storage
container and databricks file system.

Attributes:
    DBFS_FS_PREFIX (str): The databricks file system api prefix.
    DBFS_LOCAL_ROOT (str): The databricks Ubuntu machine file prefix.
"""

from datetime import datetime
import json
import os
from typing import List
import tempfile
from src.config import logger
import zipfile_deflate64 as zipfile
from typing import IO, Union
import yaml
import pandas as pd


DBFS_FS_PREFIX = "/dbfs/"
DBFS_DB_PREFIX = "dbfs:"
DBFS_LOCAL_ROOT = "file:"


class Loader(yaml.SafeLoader):
    """YAML Loader with `!include` constructor."""

    def __init__(self, stream: IO) -> None:
        """Initialise Loader."""

        try:
            self._root = os.path.split(stream.name)[0]
        except AttributeError:
            self._root = os.path.curdir

        super().__init__(stream)


def load_yaml(path: str):
    with open(path) as f:
        return yaml.load(f, Loader)


def load_json(path: str):
    with open(path) as f:
        return json.load(f)


def unzip_file_to_storage_container(dbutils, sa_path: str) -> None:
    """Unzips files from in Azure storage container.

    Args:
        dbutils (Any): utilities package provided by Databricks environment.
        sa_path (str): storage account path.

    Raises:
        Exception: If files fail to unzip.
    """
    try:
        for zip_file in dbutils.fs.ls(sa_path):
            if zip_file.path.endswith(".zip"):
                file_name = zip_file.name
                with tempfile.TemporaryDirectory(
                    prefix=DBFS_FS_PREFIX
                ) as directory:
                    dest_path = f"{directory}/{file_name}"
                    dbutils.fs.mv(f"{sa_path}{file_name}", dest_path)
                    logger.info(
                        f"Successfully copied file from {sa_path}{file_name} to {dest_path}"  # noqa
                    )
                    with zipfile.ZipFile(
                        f"{DBFS_FS_PREFIX}{dest_path}"
                    ) as zfile:
                        zfile.extractall(f"{DBFS_FS_PREFIX}{directory}")
                        for data_file in zfile.infolist():
                            dbutils.fs.mv(
                                f"{directory}/{data_file.filename}", sa_path
                            )
                            logger.info("Successfully written to file")
                logger.info(
                    f"All files successfully unzipped archiving {zip_file.name}"  # noqa
                )
    except Exception as error:
        logger.exception("Failed to unzip files")
        raise error


def zip_files_to_storage_account(
    dbutils, sa_path: str, exclusion_list: List[str]
) -> None:
    """Compresses files into Azure storage account.

    Args:
        dbutils (Any): utilities package provided by Databricks environment.
        sa_path (str): storage account path.
        exclusion_list (list[str]): list of files to exclude from compressing.

    Raises:
        Exception: failed to move file from azure storage to dbfs.
        Exception: failed to zip files.
    """
    with tempfile.TemporaryDirectory(prefix=DBFS_FS_PREFIX) as directory:
        logger.info(f"Created temp directory at {directory}")
        try:
            dest_path = directory
            for blob_file in dbutils.fs.ls(sa_path):
                if blob_file.name not in exclusion_list:
                    dest_path = f"{directory}/{blob_file.name}"
                    dbutils.fs.mv(f"{sa_path}{blob_file.name}", dest_path)
                    logger.info(f"Prepare {blob_file.name} for zipping")
        except Exception as error:
            logger.exception(f"Failed to move blob to {dest_path}")
            raise error
        try:
            local_path = "/tmp/temp_zip.zip"
            with zipfile.ZipFile(
                local_path,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=6,
            ) as zfile:
                for file_name in os.listdir(f"{DBFS_FS_PREFIX}{directory}"):
                    zfile.write(f"{DBFS_FS_PREFIX}{directory}/{file_name}")
                    logger.info(f"File {file_name} has been added to zip")
            dbutils.fs.mv(
                f"{DBFS_LOCAL_ROOT}{local_path}",
                zip_file := f"{sa_path}{sa_path.split('/')[-2]}_{datetime.now()}.zip",  # noqa
            )
            logger.info(
                f"Files in {sa_path} have been successfully zipped to {zip_file}"  # noqa
            )
        except Exception as error:
            logger.exception("Failed to zip files")
            raise error


def save_pandas_csv_to_storage(
    dbutils, data: pd.DataFrame, file_name: str, abfss_path: str
):
    with tempfile.TemporaryDirectory(prefix=DBFS_FS_PREFIX) as directory:
        tmp_dir = directory.split("/")[2]
        db_path = f"{DBFS_DB_PREFIX}/{tmp_dir}/{file_name}"
        data.to_csv(f"{directory}/{file_name}")
        dbutils.fs.mv(db_path, abfss_path)
