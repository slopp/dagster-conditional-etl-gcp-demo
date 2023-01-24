import ast

import pandas as pd
from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    Definitions,
    GraphOut,
    Out,
    Output,
    RunRequest,
    SkipReason,
    asset,
    build_resources,
    define_asset_job,
    fs_io_manager,
    graph,
    op,
    sensor,
    multi_asset,
    AssetOut,
    DailyPartitionsDefinition
)
from dagster._config.structured_config import Config
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError

from .resources import DirectoryLister, FileReader

# --- Resources
duckdb = duckdb_pandas_io_manager.configured({"database": "example.duckdb"})

def get_env():
    return "LOCAL"

resources = {
    "LOCAL":  {
        "file_reader": FileReader(),
        "warehouse_io_manager": duckdb,
        "ls": DirectoryLister(),
        "dbt": dbt_cli_resource.configured(
            {"project_dir": "dbt_project", "profiles_dir": "dbt_project/config"}
        ),
        "failure_io": fs_io_manager,
    },

    "GCP": {
        "file_reader": FileReader(),
        "warehouse_io_manager": duckdb,
        "ls": DirectoryLister(),
        "dbt": dbt_cli_resource.configured(
            {"project_dir": "dbt_project", "profiles_dir": "dbt_project/config", "target": "GCP"}
        ),
        "failure_io": fs_io_manager,
    }
}
   

# --- Assets

PlantDataSchema = DataFrameSchema(
    {
        "plant": Column(str),
        "date": Column(str),
        "part_number": Column(int, Check(lambda s: s < 10)),
        "quantity": Column(int),
    }
)

class PlantDataConfig(Config):
    file: str

# OPTION 1: ASSET WITHOUT BRANCHING
@asset(
    io_manager_key="warehouse_io_manager",
    key_prefix=["vehicles"],
    group_name="vehicles"
)
def plant_data(config: PlantDataConfig, file_reader: FileReader):
    data = file_reader.read(config.file)
    return data

# OPTION 2: CONDITIONAL BRANCHING ASSET
@multi_asset(
    outs={
        "plant_data_good": AssetOut(is_required=False, io_manager_key="warehouse_io_manager", group_name="branching"),
        "plant_data_bad": AssetOut(is_required=False, io_manager_key="failure_io", group_name="branching"),
    }
)
def plant_data_conditional(context, config: PlantDataConfig, file_reader: FileReader):
    data = file_reader.read(config.file)
    try:
        PlantDataSchema.validate(data)
        yield Output(data, "plant_data_good", metadata={"schema_check": "Passed Validation"} )
    except SchemaError as e:
        context.log.warn("Plant data failed schema check!")
        yield Output(data, "plant_data_bad", metadata={"schema_check": f"Failed Validation with {str(e)}"})


dbt_assets = load_assets_from_dbt_project(
    project_dir="dbt_project", profiles_dir="dbt_project/config"
)

# OPTION 3: PARTITIONS
@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-01-20"),
    io_manager_key="warehouse_io_manager",
    metadata={'partition_expr': 'date'},
    key_prefix=["vehicles"],
    group_name="partitions"
)
def plant_data_partitioned(context, file_reader: FileReader): 
    partition =  context.asset_partition_key_for_output()
    if partition == "2023-01-20":
        file = "parts.csv"
    elif partition == "2023-01-22":
        file = "bad_parts.csv"
    else:
        file = None
    data = file_reader.read(file)
    return data

# --- Jobs
asset_job = define_asset_job(
    name="new_plant_data_job",
    selection=AssetSelection.all() - AssetSelection.assets(plant_data),
)

# --- Sensors
@sensor(job=asset_job)
def watch_for_new_plant_data(context):

    ls = DirectoryLister()
    cursor = context.cursor or None
    already_seen = set()
    if cursor is not None:
        already_seen = set(ast.literal_eval(cursor))

    files = set(ls.list("data"))
    new_files = files - already_seen

    if len(new_files) == 0:
        return SkipReason("No new files to process")

    run_requests = [
        RunRequest(
            run_key=file,
            run_config={
                "ops": {
                     "plant_data_conditional": {
                                "config": {"file": f"data/{file}"}
                            }
                    }
                }
        )
        for file in new_files
    ]
    context.update_cursor(str(files))
    return run_requests


defs = Definitions(
    assets=[plant_data_conditional, plant_data, plant_data_partitioned, *dbt_assets],
    resources=resources[get_env()],
    jobs=[asset_job],
    sensors=[watch_for_new_plant_data],
)
