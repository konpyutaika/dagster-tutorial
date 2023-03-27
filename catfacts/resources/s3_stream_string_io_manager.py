from io import BytesIO,StringIO
from typing import Optional, Union, Sequence,Callable, Iterable as _Iterable
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
import os

def to_sized_file_stream(line_stream: _Iterable[str], file_mb_size: int = 100) -> _Iterable[str]:
    """
    Assembles a stream of lines into fixed file stream
    """
    file = StringIO()
    file_bytes_size = 10**6 * file_mb_size
    has_returned_file = False
    for line in line_stream:
        if file.tell():
            file.write("\n")
        file.write(line)
        if file.tell() >= file_bytes_size:
            has_returned_file = True
            yield file.getvalue()
            file = StringIO()
    if file.tell() > 0 or not has_returned_file:
        yield file.getvalue()


class S3StreamStringIOManager(IOManager):
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

    def handle_output(self, context: "OutputContext", obj: Callable) -> None:
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        path = self._uri_for_key(key)

        context.log.debug(f"Writing S3 object at: {path}")

        if self._has_object(key):
            context.log.warning(f"Removing existing S3 key: {key}")
            self._rm_object(key)

        response = self.s3.create_multipart_upload(Bucket=self.s3_bucket, Key=key)
        lines = to_sized_file_stream(obj(),6)
        upload_id = response['UploadId']
        etags = []
        for part_number, line in enumerate(lines, start=1):
            response = self.s3.upload_part(Bucket=self.s3_bucket, Key=key, UploadId=upload_id,PartNumber=part_number, Body=line)
            etags.append({'PartNumber': part_number, 'ETag': response['ETag']})

        self.s3.complete_multipart_upload(Bucket=self.s3_bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': etags})
        context.add_output_metadata({"uri": MetadataValue.path(path)})

class S3PathIOManager(S3StreamStringIOManager):

    def load_input(self, context: "InputContext") -> Optional[str]:
        asset_key = context.asset_key
        event_log_entry = context.step_context.instance.get_latest_materialization_event(asset_key)
        metadata = event_log_entry.dagster_event.event_specific_data.materialization.metadata
        return metadata.get("uri").value




@io_manager(
    config_schema={
        "s3_bucket": Field(StringSource),
        "s3_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"s3"},
)
def s3_stream_string_io_manager(init_context):
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config["s3_bucket"]
    s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
    return S3StreamStringIOManager(s3_bucket, s3_session, s3_prefix=s3_prefix)


@io_manager(
    config_schema={
        "s3_bucket": Field(StringSource),
        "s3_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"s3"},
)
def s3_path_io_manager(init_context):
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config["s3_bucket"]
    s3_prefix = init_context.resource_config.get("s3_prefix")  # s3_prefix is optional
    return S3PathIOManager(s3_bucket, s3_session, s3_prefix=s3_prefix)