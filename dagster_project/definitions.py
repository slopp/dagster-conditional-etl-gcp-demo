from dagster import Definitions, asset, sensor, define_asset_job, SkipReason, RunRequest, build_resources, AssetSelection
from dagster._config.structured_config import Config
import pandas as pd
from dagster_duckdb_pandas import  duckdb_pandas_io_manager
from .resources import FileReader, DirectoryLister
import ast
from dagster_dbt import load_assets_from_dbt_project, dbt_cli_resource

# --- Resources
duckdb = duckdb_pandas_io_manager.configured({
    "database": "example.duckdb"
})


# --- Assets

class PlantDataConfig(Config):
    file: str

@asset(
    io_manager_key="warehouse_io_manager",
    key_prefix=["vehicles"],
    group_name="vehicles"
)
def plant_data(config: PlantDataConfig, file_reader: FileReader):
    data = file_reader.read(config.file)
    return data

dbt_assets = load_assets_from_dbt_project(
    project_dir="dbt_project",
    profiles_dir="dbt_project/config"
)

# --- Jobs 
asset_job = define_asset_job(
    name = "new_plant_data_job",
    selection = AssetSelection.all(),    
)

# --- Sensors 

@sensor(
    job = asset_job
)
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
        
        run_requests = [RunRequest(run_key = file, run_config={"ops": {"vehicles__plant_data": {"config": {"file": f"data/{file}" }}}}) for file in new_files]
        context.update_cursor(str(files))
        return run_requests   

defs = Definitions(
    assets = [plant_data, *dbt_assets],
    resources = {
        "file_reader": FileReader(),
        "warehouse_io_manager": duckdb, 
        "ls": DirectoryLister(),
        "dbt": dbt_cli_resource.configured({"project_dir": "dbt_project", "profiles_dir": "dbt_project/config"})
    },
    jobs=[asset_job],
    sensors=[watch_for_new_plant_data]
)