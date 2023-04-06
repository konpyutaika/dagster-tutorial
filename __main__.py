from dagster import RunConfig

from catfacts import defs
from catfacts.jobs import job_name
from catfacts.resources.api_config import ApiConfig
from catfacts.resources.s3_config import S3Config


if __name__ == "__main__":
    result = defs.get_job_def("redshift").execute_in_process()
