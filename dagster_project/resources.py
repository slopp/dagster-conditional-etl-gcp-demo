import os
from typing import Optional

import pandas as pd
import pandas_gbq
from dagster._config.structured_config import Resource, StructuredConfigIOManager
from google.cloud import storage


class FileReader(Resource):
    """Reads CSVs from a specific folder"""

    directory: Optional[str]

    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(f"{self.directory}/{file}")


class ErrorWriter(Resource):
    """Writes failed dataframes to a specific folder"""

    directory: Optional[str]

    def write(self, obj: pd.DataFrame, file: str):
        return obj.to_csv(f"{self.directory}/{file}")


class DirectoryLister(Resource):
    """Lists files in a directory"""

    directory: Optional[str]

    def list(self):
        return os.listdir(self.directory)


class GCPFileReader(FileReader):
    """Reads CSVs from a GCS bucket"""

    bucket: str

    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(f"{self.bucket}/{file}")


class GCPErrorWriter(ErrorWriter):
    """Writes failed dataframes to a GCS bucket in a specific folder"""

    bucket: str
    folder: str

    def write(self, obj: pd.DataFrame, file: str):
        obj.to_csv(f"{self.bucket}/{self.folder}/{file}")
        return


class GCPDirectoryLister(DirectoryLister):
    """Lists blobs in a GCS bucket except any files in an ignored folder"""

    bucket: str
    ignore_folder: str

    def list(self):
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(self.bucket)
        files = []
        for blob in blobs:
            if self.ignore_folder in blob.name:
                continue
            files.append(blob.name)
        return files


class BQIOManager(StructuredConfigIOManager):
    """An IO Manager that appends dataframes to a table named by the asset key."""

    dataset: str

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        dataset_table = f"{self.dataset}.{context.asset_key.path[-1]}"
        pandas_gbq.to_gbq(obj, dataset_table, if_exists="append")

        context.add_output_metadata(
            {"sql": f"SELECT * FROM {dataset_table}", "nrow": len(obj)}
        )

        return

    def load_input(self, context) -> pd.DataFrame:
        # TODO
        pass
