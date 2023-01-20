import os

import pandas as pd
from dagster._config.structured_config import Resource


class FileReader(Resource):
    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(file)


class DirectoryLister(Resource):
    def list(self, dir):
        return os.listdir(dir)
