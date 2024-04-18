from pytest import raises, fixture
from src.utils.file_handler import (
    unzip_file_to_storage_container,
    zip_files_to_storage_account,
)
from tests.src.utils.mock_dbutils import DBUtils
import zipfile
import os

dbutils = DBUtils()


@fixture
def setup_DBFS_prefixes(tmp_path, monkeypatch):
    monkeypatch.setattr("src.utils.file_handler.DBFS_FS_PREFIX", str(tmp_path))
    monkeypatch.setattr("src.utils.file_handler.DBFS_FS_PREFIX", "")
    monkeypatch.setattr("src.utils.file_handler.DBFS_LOCAL_ROOT", "")


def test_successful_unzip_file_to_storage_container(
    tmp_path, setup_DBFS_prefixes
):
    zip_file_location = tmp_path / "src"
    zip_file_location.mkdir()
    with open(raw_file := tmp_path / "test1.txt", "w") as file_to_write:
        file_to_write.write("test")
    with zipfile.ZipFile(zip_file_location / "test1.zip", "w") as zip_file:
        zip_file.write(raw_file)

    unzip_file_to_storage_container(dbutils, f"{str(zip_file_location)}/")
    assert (
        not os.path.isfile(zip_file_location / "test1.zip")
    ) and os.path.isfile(zip_file_location / "test1.txt")


def test_unsuccessful_file_move_from_src_unzip_file_to_storage_container(
    tmp_path, setup_DBFS_prefixes
):
    with raises(Exception) as error_info:
        unzip_file_to_storage_container(dbutils, "fake_location")
    assert error_info.value.args[0] == ("The specified path does not exist.")


def test_successful_zip_files_to_storage_account(
    tmp_path, setup_DBFS_prefixes
):
    txt_file_location = tmp_path / "src"
    txt_file_location.mkdir()
    tmp_file_location = tmp_path / "tmp"
    tmp_file_location.mkdir()
    archive_file_location = txt_file_location / "archive"
    archive_file_location.mkdir()

    with open(txt_file_location / "test1.txt", "w") as file_to_write:
        file_to_write.write("test")

    zip_files_to_storage_account(
        dbutils, f"{str(txt_file_location)}/", exclusion_list=["archive/"]
    )
    assert (
        not os.path.isfile(txt_file_location / "test1.txt")
    ) and os.path.isdir(archive_file_location)


def test_unsuccessful_zip_files_to_storage_account(setup_DBFS_prefixes):
    with raises(Exception) as error_info:
        zip_files_to_storage_account(
            dbutils, "fake_location/", exclusion_list=["archive/"]
        )
    assert error_info.value.args[0] == ("The specified path does not exist.")
