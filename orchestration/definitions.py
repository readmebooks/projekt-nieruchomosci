import os
from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource

# Importujemy nasz asset z nowego pliku
from orchestration.assets.dbt_assets import uk_property_assets, DBT_PROJECT_DIR

# Ścieżka do profiles.yml (root projektu)
PROFILES_DIR = Path(__file__).joinpath("..", "..").resolve()

defs = Definitions(
    assets=[uk_property_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_DIR),
            profiles_dir=os.fspath(PROFILES_DIR), 
        ),
    },
)