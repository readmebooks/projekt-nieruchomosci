import os
from pathlib import Path
from dagster import Definitions, AssetSelection, define_asset_job
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt_assets import uk_property_assets, DBT_PROJECT_DIR
from orchestration.assets.streaming_assets import raw_stream_ingestion
from orchestration.sensors.queue_sensor import redpanda_message_sensor # Import sensor

PROFILES_DIR = Path(__file__).joinpath("..", "..").resolve()

# We need to define a job that the sensor will trigger
streaming_job = define_asset_job(
    name="streaming_ingestion_job",
    selection=AssetSelection.assets(raw_stream_ingestion)
)

defs = Definitions(
    assets=[uk_property_assets, raw_stream_ingestion],
    jobs=[streaming_job],
    sensors=[redpanda_message_sensor], # Add sensor here
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_DIR),
            profiles_dir=os.fspath(PROFILES_DIR), 
        ),
    },
)