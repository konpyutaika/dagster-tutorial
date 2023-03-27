from dagster import AssetSelection, define_asset_job

from .assets import CATS, AIRFLOW_METADATA


job_name = "catfacts_job"
catfacts_job = define_asset_job(
    name=job_name,
    selection=AssetSelection.groups(AIRFLOW_METADATA),
    tags={
        "job": job_name
    },
)