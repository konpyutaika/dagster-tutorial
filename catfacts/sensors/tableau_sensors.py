from dagster import AssetKey, EventLogEntry, SensorEvaluationContext, asset_sensor, RunRequest
from ..jobs import tableau_job
from catfacts.resources.s3_config import S3Config


@asset_sensor(asset_key=AssetKey("redshift_table"), job=tableau_job)
def my_asset_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    yield RunRequest(
        run_key=context.cursor,
        run_config={
            "resources": {
                "io_manager": dict(config=dict(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="catfacts",
                )),
                "stream_string_io_manager": dict(config=dict(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="local",
                )),
                "s3_path_io_manager": dict(config=dict(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="local",
                ))
            }
        },
    )
