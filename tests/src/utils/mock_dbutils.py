import shutil
import os
from datetime import datetime
from time import mktime


class FileInfo:
    def __init__(
        self,
        path,
        name,
        size,
        modificationTime=mktime(datetime.now().timetuple()),
    ) -> None:
        self.path = path
        self.name = name
        self.size = size
        self.modificationTime = modificationTime

    def __repr__(self) -> str:
        return f"FileInfo(path={self.path}, name={self.name}, size={self.size}, modificationTime={self.modificationTime})"


class DBUtils:
    class DBFSUtils:
        def mv(self, src, dest):
            if src.startswith("dbfs"):
                src = src[5:]
            try:
                shutil.move(src, dest)
                return True
            except Exception as e:
                print(e)
                return False

        def ls(self, path):
            for wpath, folders, files in os.walk(path):
                file_info_list = []
                for folder in folders:
                    file_info_list.append(
                        FileInfo(f"{wpath}/{folder}/", f"{folder}/", 0)
                    )
                for file in files:
                    file_info_list.append(
                        FileInfo(
                            f"{wpath}/{file}",
                            file,
                            os.path.getsize(f"{wpath}/{file}"),
                        )
                    )

                return file_info_list
            raise Exception("The specified path does not exist.")

    def __init__(self) -> None:
        self.fs = self.DBFSUtils()
