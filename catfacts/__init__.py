import os

from dagster import Definitions, ScheduleDefinition

from .assets import cats_assets, gsheet_assets, redshift_assets
from .jobs import catfacts_job, gsheet_job, redshift_job
from .resources import RESOURCES_LOCAL, RESOURCES_STAGING, RESOURCES_PROD
from .sensors.redshift import redshift_sensor

all_assets = [*cats_assets, *gsheet_assets, *redshift_assets]

resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "dev": RESOURCES_LOCAL,
}

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name[os.getenv("ENV", "dev")],
    jobs=[catfacts_job, gsheet_job, redshift_job],
    sensors=[redshift_sensor],
    # schedules=[ScheduleDefinition(job=gsheet_job, cron_schedule="28 * * * *")]
)
