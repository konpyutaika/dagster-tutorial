# https://github.com/dagster-io/dagster/discussions/11653
import io
from typing import Optional, Union, Sequence, Mapping, List, Dict, Any

import pandas as pd
from dagster import (
    InputContext,
    InputManager,
    OutputContext,
    MetadataValue,
    TableSchema,
    TableRecord,
    TableColumn,
    StringSource,
    Field,
    input_manager,
    io_manager,
)
from dagster_aws.s3.io_manager import PickledObjectS3IOManager


def _get_path(context: Union[InputContext, OutputContext], s3_prefix: str) -> Union[str, Mapping[str, str]]:
    path: Sequence[str]

    if not context.has_asset_partitions:
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = ["storage", *context.get_identifier()]
        return "/".join([s3_prefix, *path])
    print(f"okok {s3_prefix}")
    return {
        partition_key: "/".join([s3_prefix, *[*context.asset_key.path, partition_key]])
        for partition_key in context.asset_partition_keys
    }


def _uri_for_key(key: str, bucket: str):
    return "s3://" + bucket + "/" + "{key}".format(key=key)


class S3PandaCsvIOManager(PickledObjectS3IOManager):

    def _uri_for_key(self, key):
        return _uri_for_key(key, self.bucket)

    def _get_path(self, context: Union[InputContext, OutputContext]) -> Union[str, Mapping[str, str]]:
        return _get_path(context, self.s3_prefix)

    def load_input(self, context: "InputContext") -> Optional[Dict[str, pd.DataFrame]]:
        if context.dagster_type.typing_type == type(None):
            return None
        keys = self._get_path(context)

        # No partition or only one partition
        if isinstance(keys, str) or len(keys) == 1:
            partition_key: str = "default"
            if not isinstance(keys, str):
                partition_key = next(iter(keys))
                keys = keys[partition_key]
            context.log.debug(f"Loading S3 object from: {self._uri_for_key(keys)}")
            return {partition_key: pd.read_csv(self.s3.get_object(Bucket=self.bucket, Key=keys).get("Body"), squeeze=True)}

        return {
            partition_key: pd.read_csv(self.s3.get_object(Bucket=self.bucket, Key=keys[partition_key]).get("Body"), squeeze=True)
            for partition_key in keys
        }

    def _handle_dataframe_output(self, key: str, context: "OutputContext", obj: pd.DataFrame) -> str:

        path = self._uri_for_key(key)

        context.log.debug(f"Writing S3 object at: {path}")

        if self._has_object(key):
            context.log.warning(f"Removing existing S3 key: {key}")
        self._rm_object(key)

        csv_buffer = io.StringIO()
        obj.to_csv(csv_buffer, index=False)
        self.s3.put_object(Body=csv_buffer.getvalue(), Bucket=self.bucket, Key=key)
        return path

    def handle_output(self, context: "OutputContext", obj: Dict[str, pd.DataFrame]) -> None:

        if context.dagster_type.typing_type == type(None):
            return None
        keys = self._get_path(context)

        # No partition or only one partition
        if isinstance(keys, str) or len(obj) == 1:
            partition_key = next(iter(obj))
            content = obj[partition_key]
            if not isinstance(keys, str):
                keys = keys[partition_key]
            path = self._handle_dataframe_output(
                key=keys,
                context=context,
                obj=content
            )
            context.add_output_metadata({"uri": MetadataValue.path(path)})
            return

        uris: List[TableRecord] = []
        print(keys)
        for key in keys:
            if key not in obj:
                continue
            path = self._handle_dataframe_output(
                key=keys[key],
                context=context,
                obj=obj[key]
            )
            uris.append(TableRecord(data={"uri": path, "key": key}))

        # https://github.com/dagster-io/dagster/issues/12498
        context.add_output_metadata({
            "uris": MetadataValue.table(
                records=uris,
                schema=TableSchema(columns=[
                    TableColumn(name="key"),
                    TableColumn(name="uri"),
                ])
            )
        })


@io_manager(
    config_schema={
        "s3_bucket": Field(StringSource),
        "s3_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"s3"},
)
def s3_pd_csv_io_manager(init_context):
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config["s3_bucket"]
    s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
    return S3PandaCsvIOManager(s3_bucket, s3_session, s3_prefix=s3_prefix)


class S3PathInputLoader(InputManager):
    def __init__(
            self,
            s3_bucket,
            s3_prefix=None,
    ):
        self.bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def load_input(self, context: "InputContext") -> Optional[Dict[str, str]]:
        if context.dagster_type.typing_type == type(None):
            return None
        keys = _get_path(context, self.s3_prefix)

        # No partition or only one partition
        if isinstance(keys, str) or len(keys) == 1:
            partition_key: str = "default"
            if not isinstance(keys, str):
                partition_key = next(iter(keys))
                keys = keys[partition_key]
            context.log.debug(f"Loading S3 object from: {_uri_for_key(key=keys, bucket=self.bucket)}")
            return {partition_key: _uri_for_key(key=keys, bucket=self.bucket)}

        return {
            partition_key: _uri_for_key(key=keys[partition_key], bucket=self.bucket)
            for partition_key in keys
        }


@input_manager(
    config_schema={
        "s3_bucket": Field(StringSource),
        "s3_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
)
def s3_path_input_loader(init_context):
    s3_bucket = init_context.resource_config["s3_bucket"]
    s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
    return S3PathInputLoader(s3_bucket, s3_prefix=s3_prefix)
