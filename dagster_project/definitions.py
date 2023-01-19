from dagster import Definitions, asset
from dagster._config.structured_config import Config
import pandas as pd
from dagster_duckdb_pandas import  duckdb_pandas_io_manager
from .resources import FileReader

duckdb = duckdb_pandas_io_manager.configured({
    "database": "example.duckdb",
    "schema": "vehicles"
})

class PlantDataConfig(Config):
    file: str

@asset(
    io_manager_key="warehouse_io_manager"
)
def plant_data(config: PlantDataConfig, file_reader: FileReader):
    data = file_reader.read(config.file)
    return data



defs = Definitions(
    assets = [plant_data],
    resources = {
        "file_reader": FileReader(),
        "warehouse_io_manager": duckdb
    }
)