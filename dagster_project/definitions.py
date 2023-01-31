import ast
import base64
import json
import os

from dagster import (
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError

from .resources import (
    BQIOManager,
    DirectoryLister,
    ErrorWriter,
    FileReader,
    GCPDirectoryLister,
    GCPErrorWriter,
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

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE

        return "GCP"

    return "LOCAL"


resources = {
    "LOCAL": {
        "file_reader": FileReader(directory="data"),
        "error_writer": ErrorWriter(directory="failed"),
        "warehouse_io_manager": duckdb_pandas_io_manager.configured(
            {"database": "example.duckdb"}
        ),
        "dbt": dbt_cli_resource.configured(
            {"project_dir": "dbt_project", "profiles_dir": "dbt_project/config"}
        ),
    },
    "GCP": {
        "file_reader": GCPFileReader(bucket="gcs://hooli-demo"),
        "error_writer": GCPErrorWriter(bucket="gcs://hooli-demo", folder="failed"),
        "warehouse_io_manager": BQIOManager(dataset="hooli"),
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

# --- Assets
@asset(
    io_manager_key="warehouse_io_manager",
    partitions_def=DailyPartitionsDefinition(start_date="2023-01-20"),
    metadata={"partition_expr": "date"},
    key_prefix=["vehicles"],
    group_name="vehicles",
)
def plant_data(context, file_reader: FileReader, error_writer: ErrorWriter):
    """
    Plant data that matches a schema requirement

    Raw data is read from directory, 1 file per day
    These daily files are represented as partitions

    If data fails the schema check the partition fails
    which can trigger an alert and writes the bad data
    to a failed location
    """

    PlantDataSchema = DataFrameSchema(
        {
            "plant": Column(str),
            "date": Column(str),
            "part_number": Column(int, Check(lambda s: s < 10)),
            "quantity": Column(int),
        }
    )

    file = f"{context.asset_partition_key_for_output()}.csv"
    data = file_reader.read(file)

    try:
        PlantDataSchema.validate(data)
    except SchemaError as e:
        context.log.warn(
            f"Plant data failed schema check! Writing bad data to failed/{file}"
        )
        error_writer.write(data, file)
        raise e

    return data


# downstream assets managed by dbt
dbt_assets = load_assets_from_dbt_project(
    project_dir="dbt_project", profiles_dir="dbt_project/config"
)

# --- Jobs
asset_job = define_asset_job(
    name="new_plant_data_job",
    selection=AssetSelection.all(),
    partitions_def=DailyPartitionsDefinition(start_date="2023-01-20"),
)

# --- Sensors
@sensor(job=asset_job)
def watch_for_new_plant_data(context):
    """Sensor watches for new data files to be processed"""

    ls = sensor_resources[get_env()]
    cursor = context.cursor or None
    already_seen = set()
    if cursor is not None:
        already_seen = set(ast.literal_eval(cursor))

    files = set(ls.list())
    new_files = files - already_seen

    if len(new_files) == 0:
        return SkipReason("No new files to process")

    run_requests = [
        asset_job.run_request_for_partition(
            partition_key=file.replace(".csv", ""), run_key=file
        )
        for file in new_files
    ]
    context.update_cursor(str(files))
    return run_requests


defs = Definitions(
    assets=[plant_data, *dbt_assets],
    resources=resources[get_env()],
    jobs=[asset_job],
    sensors=[watch_for_new_plant_data],
)
