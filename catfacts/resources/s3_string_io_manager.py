from io import BytesIO
from typing import Optional, Union, Sequence

import pandas as pd
from dagster import (
    InputContext,
    OutputContext,
    MetadataValue,
    IOManager,
    StringSource,
    Field,
    _check as check,
    io_manager
)
import pyarrow.parquet as pq
import pyarrow as pa


class S3StringIOManager(IOManager):
    def __init__(
            self,
            s3_bucket,
            s3_session,
            s3_prefix=None,
    ):
        self.s3_bucket = check.str_param(s3_bucket, "s3_bucket")
        self.s3_prefix = check.opt_str_param(s3_prefix, "s3_prefix")
        self.s3 = s3_session
        self.s3.list_objects(Bucket=self.s3_bucket, Prefix=self.s3_prefix, MaxKeys=1)

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        path: Sequence[str]
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = ["storage", *context.get_identifier]

        return "/".join([self.s3_prefix, *path])

    def _rm_object(self, key):
        self.s3.delete_object(Bucket=self.s3_bucket, Key=key)

    def _has_object(self, key):
        try:
            self.s3.get_object(Bucket=self.s3_bucket, Key=key)
            found_object = True
        except self.s3.exceptions.NoSuchKey:
            found_object = False

        return found_object

    def _uri_for_key(self, key):
        return "s3://" + self.s3_bucket + "/" + "{key}".format(key=key)

    def load_input(self, context: "InputContext") -> Optional[str]:
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        context.log.debug(f"Loading S3 object from: {self._uri_for_key(key)}")
        return self.s3.get_object(Bucket=self.s3_bucket, Key=key)["Body"].read().decode('utf-8')

    def handle_output(self, context: "OutputContext", obj: str) -> None:
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        path = self._uri_for_key(key)

        context.log.debug(f"Writing S3 object at: {path}")

        if self._has_object(key):
            context.log.warning(f"Removing existing S3 key: {key}")
            self._rm_object(key)

        self.s3.put_object(Body=obj, Bucket=self.s3_bucket, Key=key)
        context.add_output_metadata({"uri": MetadataValue.path(path)})


@io_manager(
    config_schema={
        "s3_bucket": Field(StringSource),
        "s3_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"s3"},
)
def s3_string_io_manager(init_context):
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config["s3_bucket"]
    s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
    return S3StringIOManager(s3_bucket, s3_session, s3_prefix=s3_prefix)
