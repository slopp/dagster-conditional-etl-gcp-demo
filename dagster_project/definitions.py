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
)
from dagster._config.structured_config import Config
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError

from .resources import DirectoryLister, FileReader

# --- Resources
duckdb = duckdb_pandas_io_manager.configured({"database": "example.duckdb"})


# --- Assets

# ORIGINAL ASSET WITHOUT BRANCHING
# @asset(
#     io_manager_key="warehouse_io_manager",
#     key_prefix=["vehicles"],
#     group_name="vehicles"
# )
# def plant_data(config: PlantDataConfig, file_reader: FileReader):
#     data = file_reader.read(config.file)
#     return data

# BEGIN CONDITIONAL BRANCHING ASSET
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


@op(
    out={
        "plant_data_good": Out(is_required=False),
        "plant_data_bad": Out(is_required=False),
    }
)
def plant_data_conditional(context, config: PlantDataConfig, file_reader: FileReader):
    data = file_reader.read(config.file)
    try:
        PlantDataSchema.validate(data)
        yield Output(
            data, "plant_data_good", metadata={"schema_check": "Passed Validation"}
        )
    except SchemaError as e:
        context.log.warn("Plant data failed schema check!")
        yield Output(
            data, "plant_data_bad", metadata={"schema_check": f"Failed with {str(e)}"}
        )


@op(out=Out(io_manager_key="failure_io"))
def plant_data_bad_op(data):
    return data


@op(out=Out(io_manager_key="warehouse_io_manager"))
def plant_data_good_op(data):
    return data


@graph(out={"plant_data": GraphOut(), "failed_plant_data": GraphOut()})
def plant_data_graph():
    plant_data_good, plant_data_bad = plant_data_conditional()
    return {
        "plant_data": plant_data_good_op(plant_data_good),
        "failed_plant_data": plant_data_bad_op(plant_data_bad),
    }


plant_data = AssetsDefinition.from_graph(
    plant_data_graph, key_prefix="vehicles", group_name="raw_data"
)
# END CONDITIONAL BRANCHING ASSET

dbt_assets = load_assets_from_dbt_project(
    project_dir="dbt_project", profiles_dir="dbt_project/config"
)

# --- Jobs
asset_job = define_asset_job(
    name="new_plant_data_job",
    selection=AssetSelection.all(),
)

# --- Sensors
@sensor(job=asset_job)
def watch_for_new_plant_data(context):
    with build_resources({"ls": DirectoryLister()}) as resources:

        cursor = context.cursor or None
        already_seen = set()
        if cursor is not None:
            already_seen = set(ast.literal_eval(cursor))

        files = set(resources.ls.list("data"))
        new_files = files - already_seen

        if len(new_files) == 0:
            return SkipReason("No new files to process")

        run_requests = [
            RunRequest(
                run_key=file,
                run_config={
                    "ops": {
                        "plant_data_graph": {
                            "ops": {
                                "plant_data_conditional": {
                                    "config": {"file": f"data/{file}"}
                                }
                            }
                        }
                    }
                },
            )
            for file in new_files
        ]
        context.update_cursor(str(files))
        return run_requests


defs = Definitions(
    assets=[plant_data, *dbt_assets],
    resources={
        "file_reader": FileReader(),
        "warehouse_io_manager": duckdb,
        "ls": DirectoryLister(),
        "dbt": dbt_cli_resource.configured(
            {"project_dir": "dbt_project", "profiles_dir": "dbt_project/config"}
        ),
        "failure_io": fs_io_manager,
    },
    jobs=[asset_job],
    sensors=[watch_for_new_plant_data],
)
