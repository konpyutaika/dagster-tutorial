from dagster import RunConfig

from catfacts import defs
from catfacts.jobs import job_name
from catfacts.resources.api_config import ApiConfig
from catfacts.resources.s3_config import S3Config


if __name__ == "__main__":
    result = defs.get_job_def(job_name).execute_in_process(
        run_config=RunConfig(
            #ops={
             #   "catfacts": ApiConfig(max_length=70, limit=20),
            #},
            resources={
                "io_manager": S3Config(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="catfacts",
                ),
                "stream_string_io_manager": S3Config(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="local",
                ),
                "s3_path_io_manager": S3Config(
                    s3_bucket="konpyutaika-product-catfacts-staging",
                    s3_prefix="local",
                )
                #"dataframe_io_manager": S3Config(
                #    s3_bucket="konpyutaika-product-catfacts-staging",
                #    s3_prefix="catfacts/parquet",
                #)
            }
        )
    )
