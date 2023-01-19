from dagster._config.structured_config import Resource
import pandas as pd

class FileReader(Resource):
    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(file)

