from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

from .api_resource import ApiResource
from .s3_pd_to_parquet_io_manager import s3_pd_to_parquet_io_manager

RESOURCES_LOCAL = {
    "io_manager": s3_pickle_io_manager,
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts")
}

RESOURCES_STAGING = {
    "io_manager": s3_pickle_io_manager,
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts")
}

RESOURCES_PROD = {
    "io_manager": s3_pickle_io_manager,
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts")
}
