import os

from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_aws.redshift.resources import redshift_resource
from dagster import EnvVar

from .api_resource import ApiResource
from .s3_pd_parquet_io_manager import s3_pd_to_parquet_io_manager
from .s3_pd_csv_io_manager import s3_pd_csv_io_manager, s3_path_input_loader
from .metadata_input_manager import metadata_input_loader
from ..libraries.dagster_gsheet.gsheet_resources import GsheetResource
from ..libraries.dagster_gsheet.configurable_query_resources import (
    ConfigurableQueryResource,
    ConfigurableQueryCopyResource
)

common_redshift_config = dict(
    host={"env": "REDSHIFT_CLUSTER_HOST"},
    port={"env": "REDSHIFT_CLUSTER_PORT"},
    user={"env": "REDSHIFT_CLUSTER_USER"},
    password={"env": "REDSHIFT_CLUSTER_PASSWORD"},
    database={"env": "REDSHIFT_CLUSTER_DATABASE"}
)

RESOURCES_LOCAL = {
    "io_manager": s3_pickle_io_manager.configured(dict(
        s3_bucket="konpyutaika-product-catfacts-staging",
        s3_prefix="gsheets",
    )),
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3_pd_csv_io_manager": s3_pd_csv_io_manager.configured(dict(
        s3_bucket="konpyutaika-product-catfacts-staging",
        s3_prefix="gsheets",
    )),
    "s3_path_input_loader": s3_path_input_loader.configured(dict(
        s3_bucket="konpyutaika-product-catfacts-staging",
        s3_prefix="gsheets",
    )),
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts"),
    "gsheet": GsheetResource(credentials=EnvVar("GSHEET_CREDENTIALS")),
    "redshift": redshift_resource.configured(common_redshift_config),
    "redshift_query": ConfigurableQueryResource(schema_name="aguitton_gsheet"),
    "gsheet_query": ConfigurableQueryCopyResource(
        schema_name="aguitton_gsheet",
        iam_role_arn=f"arn:aws:iam::{os.getenv('AWS_ACCOUNT_ID')}:role/RedshiftDatafoundation"
    )
}

RESOURCES_STAGING = {
    "io_manager": s3_pickle_io_manager,
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3_pd_csv_io_manager": s3_pd_csv_io_manager,
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts"),
    "gsheet": GsheetResource(credentials=EnvVar("GSHEET_CREDENTIALS")),
}

RESOURCES_PROD = {
    "io_manager": s3_pickle_io_manager,
    "dataframe_io_manager": s3_pd_to_parquet_io_manager,
    "s3_pd_csv_io_manager": s3_pd_csv_io_manager,
    "s3": s3_resource,
    "catfacts_client": ApiResource(host="catfact.ninja", protocol="https", endpoint="facts"),
    "gsheet": GsheetResource(credentials=EnvVar("GSHEET_CREDENTIALS")),
}
