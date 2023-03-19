from dagster import AssetSelection, define_asset_job

from .assets import CATS


job_name = "catfacts_job"
catfacts_job = define_asset_job(
    name=job_name,
    selection=AssetSelection.groups(CATS),
    tags={
        "job": job_name
    },
)