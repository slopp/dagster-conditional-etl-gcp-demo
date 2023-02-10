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


class ErrorIOWriter(StructuredConfigIOManager):
    """Writes failed dataframes to a specific folder"""

    directory: Optional[str]

    def handle_output(self, context, obj: pd.DataFrame):
        file = context.asset_key.path[-1]
        return obj.to_csv(f"{self.directory}/{file}")

    def load_input(self, context) -> pd.DataFrame:
        file = context.asset_key.path[-1]        
        return pd.read_csv(f"{self.directory}/{file}")


class DirectoryLister(Resource):
    """Returns a list of dicts with file name and mtime for all files in a directory"""

    directory: Optional[str]

    def list(self):
        files_mtimes = [
            {
            "name": file, 
            "mtime": os.path.getmtime(f"{self.directory}/{file}")
            } 
         for file in os.listdir(self.directory)
        ]
        return files_mtimes


class GCPFileReader(FileReader):
    """Reads CSVs from a GCS bucket"""

    bucket: str

    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(f"{self.bucket}/{file}")


class GCPErrorIOWriter(StructuredConfigIOManager):
    """Writes failed dataframes to a GCS bucket in a specific folder"""

    bucket: str
    folder: str

    def handle_output(self, context, obj: pd.DataFrame):
        file = context.asset_key.path[-1]
        obj.to_csv(f"{self.bucket}/{self.folder}/{file}")
        return

    def load_input(self, context) -> pd.DataFrame:
        file = context.asset_key.path[-1]
        return pd.read_csv(f"{self.bucket}/{self.folder}/{file}")


class GCPDirectoryLister(DirectoryLister):
    """Returns a list of dicts with file name and mtime for all files in a bucket except those in an ignored folder"""

    bucket: str
    ignore_folder: str

    def list(self):
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(self.bucket)

        files_mtimes = []

        for blob in blobs:
            if self.ignore_folder in blob.name:
                continue
            
            files_mtimes.append(
                {
                    "name": blob.name,
                    "mtime": str(blob.updated)
                }
            )
            
        return files_mtimes


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
        dataset_table = f"{self.dataset}.{context.asset_key.path[-1]}"
        return pandas_gbq.read_gbq(f"SELECT * FROM {dataset_table}")
