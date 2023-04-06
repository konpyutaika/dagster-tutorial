from dagster import AssetSelection, define_asset_job, RunConfig

from .assets import CATS, GSHEETS, REDSHIFT


job_name = "catfacts_job"
catfacts_job = define_asset_job(
    name=job_name,
    selection=AssetSelection.groups(CATS),
    tags={
        "job": job_name
    },
)

gsheet_job = define_asset_job(
    name="gsheets",
    selection=AssetSelection.groups(GSHEETS),
)

redshift_job = define_asset_job(
    name="redshift",
    selection=AssetSelection.groups(REDSHIFT),
)
