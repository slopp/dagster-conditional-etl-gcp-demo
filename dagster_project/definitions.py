import ast
import base64
import json
import os
import pandas as pd

from dagster import (
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
    multi_asset, 
    AssetOut,
    Output, 
    RunRequest,
    AssetIn
)
from dagster._config.structured_config import Config
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError

from .resources import (
    BQIOManager,
    DirectoryLister,
    ErrorIOWriter,
    FileReader,
    GCPDirectoryLister,
    GCPErrorIOWriter,
    GCPFileReader,
)

# set to true to use GCP resources
# requires GCP_CREDS_JSON_CREDS_BASE64 and 
# potential resource configuration
USE_GCP = os.getenv("USE_GCP", "False")


# --- Resources
def get_env():
    """Determine resources based on where code is running"""
    if USE_GCP == "True":
        AUTH_FILE = "./gcp_creds.json"
        with open(AUTH_FILE, "w") as f:
            json.dump(json.loads(base64.b64decode(os.getenv("GCP_CREDS_JSON_CREDS_BASE64"))), f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(AUTH_FILE)

        return "GCP"

    return "LOCAL"


resources = {
    "LOCAL": {
        "file_reader": FileReader(directory="data"),
        "error_writer": ErrorIOWriter(directory="failed"),
        "warehouse_io_manager": duckdb_pandas_io_manager.configured(
            {"database": "example.duckdb"}
        ),
        "dbt": dbt_cli_resource.configured(
            {"project_dir": "dbt_project", "profiles_dir": "dbt_project/config"}
        ),
    },
    "GCP": {
        "file_reader": GCPFileReader(bucket="gcs://hooli-demo"),
        "error_writer": GCPErrorIOWriter(bucket="gcs://hooli-demo", folder="failed"),
        "warehouse_io_manager": BQIOManager(dataset=os.getenv("GCP_BQ_DATASET")),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": "dbt_project",
                "profiles_dir": "dbt_project/config",
                "target": "GCP",
            }
        ),
    },
}

sensor_resources = {
    "LOCAL": DirectoryLister(directory="data"),
    "GCP": GCPDirectoryLister(bucket="hooli-demo", ignore_folder="failed"),
}


class FileToLoad(Config):
    file: str

@asset
def plant_data_loaded(context, config: FileToLoad, file_reader: FileReader) -> pd.DataFrame:
    data = file_reader.read(config.file)
    context.add_output_metadata(
        {
            "load_from": config.file
        }
    )
    return data

# --- Assets
@multi_asset(
    outs={
        "plant_data_good": AssetOut(is_required=False, io_manager_key="warehouse_io_manager", group_name="success_pipeline"),
        "plant_data_bad": AssetOut(is_required=False, io_manager_key="error_writer", group_name="failure_pipeline"),
    }
)
def plant_data_conditional(context, plant_data_loaded):
    """ 
        The loaded data is checked against a schema
        If the checks pass, the data is passed through to the asset "plant_data_good"
        If the checks fail, the data is passed through to the asset "plant_data_bad" 
    
    """
    
    PlantDataSchema = DataFrameSchema(
        {
            "plant": Column(str),
            "date": Column(str),
            "part_number": Column(int, Check(lambda s: s < 10)),
            "quantity": Column(int),
        }
    )


    try:
        PlantDataSchema.validate(plant_data_loaded)
        yield Output(plant_data_loaded, "plant_data_good", metadata={"schema_check": "Passed Validation"} )  
    except SchemaError as e:
        context.log.warn("Plant data failed schema check!")
        yield Output(plant_data_loaded, "plant_data_bad", metadata={"schema_check": f"Failed Validation with {str(e)}"})

@asset(
    group_name="success_pipeline",
    io_manager_key="warehouse_io_manager",
    key_prefix="vehicles"
)
def plant_data(plant_data_good: pd.DataFrame) -> pd.DataFrame:
    """ Processes the data that passed validation """
    plant_data_good['schema'] = '0.1'
    # other side affects like logging to run table go here!

    # this returned plant_data_good dataframe is written to 
    # a plant_data table using the warehouse io manager
    # and then that table is used as a dbt source 
    return plant_data_good

@asset(
    group_name="failure_pipeline",
    io_manager_key="error_writer"
)
def bad_data_queue(plant_data_bad: pd.DataFrame) -> pd.DataFrame:
    """ Processes the data that failed validation """
    # side affects like paging the team or writing to a failure queue go here!

    # the plant data bad is written to a directory / bucket using the error writer 
    plant_data_bad['schema_failed'] = '0.1'
    return plant_data_bad


# downstream assets managed by dbt
dbt_assets = load_assets_from_dbt_project(
    project_dir="dbt_project", 
    profiles_dir="dbt_project/config"
)

# --- Jobs
asset_job = define_asset_job(
    name="new_plant_data_job",
    selection=AssetSelection.all()
)

# --- Sensors
@sensor(job=asset_job)
def watch_for_new_plant_data(context):
    """Sensor watches for new or updated data files to be processed"""

    ls = sensor_resources[get_env()]

    file_mtimes = ls.list()

    # dagster sensors launch, at most, one run for each unique run_key
    # by using the file_name:mtime as the run_key, this means each file 
    # will be processed once per update
    # files that have already been processed will have a RunRequest that is skipped
    # https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#idempotence-using-run-keys

    run_requests = [
          RunRequest(
            run_key=f"{file['name']}_{file['mtime']}",
            run_config={
                "ops": {
                     "plant_data_loaded": {
                                "config": {"file": f"{file['name']}"}
                            }
                    }
                }
          )
          for file in file_mtimes
    ]
    
    return run_requests


defs = Definitions(
    assets=[plant_data, *dbt_assets, plant_data_conditional, plant_data_loaded, bad_data_queue],
    resources=resources[get_env()],
    jobs=[asset_job],
    sensors=[watch_for_new_plant_data],
)
