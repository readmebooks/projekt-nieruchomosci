import os
from pathlib import Path
from dagster import AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_transformation").resolve()

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return AssetKey([dbt_resource_props["name"]])

@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def uk_property_assets(context, dbt: DbtCliResource):
    """Assets representing the dbt transformation pipeline."""
    yield from dbt.cli(["build"], context=context).stream()