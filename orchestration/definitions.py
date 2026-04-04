import os
from dagster import Definitions
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

# Paths - automatically detect folders relative to this file
# 1. Path to the folder containing dbt_project.yml
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt_transformation").resolve()

# 2. Path to the folder containing profiles.yml (main project directory)
PROFILES_DIR = Path(__file__).joinpath("..", "..").resolve()

@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def uk_property_assets(context, dbt: DbtCliResource):
    # The build command runs models, tests, and snapshots in one go
    yield from dbt.cli(["build"], context=context).stream()

defs = Definitions(
    assets=[uk_property_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_DIR),
            profiles_dir=os.fspath(PROFILES_DIR), 
        ),
    },
)