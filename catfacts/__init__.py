import os

from dagster import Definitions

from .assets import cats_assets, airflow_metadata_assets, tableau_assets
from .sensors.tableau_sensors import my_asset_sensor
from .jobs import catfacts_job, tableau_job
from .resources import RESOURCES_LOCAL, RESOURCES_STAGING, RESOURCES_PROD

all_assets = [*cats_assets, *airflow_metadata_assets, *tableau_assets]

resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "dev": RESOURCES_LOCAL,
}

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name[os.getenv("ENV", "dev")],
    jobs=[catfacts_job, tableau_job],
    sensors=[my_asset_sensor]
)
