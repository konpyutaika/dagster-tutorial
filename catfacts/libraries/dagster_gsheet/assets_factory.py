# https://github.com/dagster-io/dagster/discussions/11045
import hashlib
from typing import Mapping, Optional, Any, Union, Sequence, Dict, Set, List

from dagster import (
    AssetsDefinition,
    AssetIn,
    FreshnessPolicy,
    IOManagerDefinition,
    Output,
    OpExecutionContext,
    PartitionsDefinition,
    ResourceDefinition,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
)
import pandas as pd

from .gsheet_resources import GsheetResource, SheetConfig
from .configurable_query_resources import (
    ConfigurableQueryCopyResource
)


def _gsheet_copy_query(target_table: str, uri: str) -> str:
    tmp_table = f"{target_table}_tmp"
    old_table = f"{target_table}_old"
    return f"""
        SET query_group TO fast_track;
        SET statement_timeout to 86400000;
    
        CREATE TABLE "{{schema_name}}"."{tmp_table}" (LIKE "gsheet"."{target_table}");
    
        COPY {{schema_name}}.{tmp_table}
        FROM '{uri}'
        CREDENTIALS 'aws_iam_role={{iam_role_arn}}'
        csv
        dateformat 'auto'
        NULL AS 'NULL'
        IGNOREHEADER 1
        TRUNCATECOLUMNS
        REGION 'eu-central-1';
    
        ALTER TABLE "{{schema_name}}"."{target_table}" RENAME TO "{old_table}";
        ALTER TABLE "{{schema_name}}"."{tmp_table}" RENAME TO "{target_table}";
        DROP TABLE IF EXISTS "{{schema_name}}"."{old_table}";
    
    
        SELECT
            tbl_rows,
            size AS table_size_mb
        FROM
            svv_table_info
        WHERE
            "schema" = '{{schema_name}}'
            AND "table" = '{target_table}';
        """


def _compute_code_version(config: Union[SheetConfig, Mapping[str, SheetConfig]]) -> str:
    if isinstance(config, SheetConfig):
        return config.code_version()
    to_hash: str = ""
    for name in config:
        to_hash = f"{to_hash}{config[name].code_version()}"
    return hashlib.sha1(to_hash.encode("utf-8")).hexdigest()


def _build_assets(
        name: str,
        config: Union[SheetConfig, Mapping[str, SheetConfig]],
        table_engine_resource_key: str,
        table_copy_resource_key: str,
        object_io_manager_key: Optional[str] = None,
        object_io_manager_def: Optional[IOManagerDefinition] = None,
        table_io_manager_key: Optional[str] = None,
        table_io_manager_def: Optional[IOManagerDefinition] = None,
        table_resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        partitions_def: Optional[PartitionsDefinition[Any]] = None,
        partition_freshness_policy: Optional[FreshnessPolicy] = None,
        op_tags: Optional[Mapping[str, Any]] = None,
        group_name: Optional[str] = None,
        output_required: bool = True,
        retry_policy: Optional[RetryPolicy] = None,
        table_input_manager_key: str = "gsheet_table_input_loader",
) -> List[AssetsDefinition]:
    @asset(
        name=f"{name}_object",
        metadata=config.metadata() if isinstance(config, SheetConfig) else None,
        description=config.description if isinstance(config, SheetConfig) else partition_freshness_policy,
        io_manager_def=object_io_manager_def,
        io_manager_key=object_io_manager_key,
        compute_kind="gsheet_to_object",
        partitions_def=partitions_def,
        op_tags=op_tags | {"kind": "gsheet_to_s3"} if op_tags is not None else {"kind": "gsheet_to_s3"},
        group_name=group_name,
        output_required=output_required,
        freshness_policy=config.object_freshness_policy if isinstance(config, SheetConfig) else None,
        retry_policy=retry_policy,
        code_version=_compute_code_version(config)
    )
    def _object_asset(
            context: OpExecutionContext,
            gsheet: GsheetResource,
    ) -> Output[Dict[str, pd.DataFrame]]:
        if isinstance(config, SheetConfig):
            sheets = {"default": gsheet.get_spreadsheet(sheet_config=config)}
            metadata = {"num_records": len(sheets)} | config.metadata()
        else:
            by_partition: Dict[str, pd.DataFrame] = {}
            for partition_key in context.asset_partition_keys_for_output():
                sheet = gsheet.get_spreadsheet(sheet_config=config[partition_key])
                by_partition[partition_key] = sheet
            sheets = by_partition
            metadata = {}

        return Output(
            value=sheets,
            # https://github.com/dagster-io/dagster/issues/12498
            metadata=metadata,
        )

    @asset(
        name=f"{name}" if isinstance(config, SheetConfig) else f"{name}_table",
        ins={
            "data": AssetIn(
                key=f"{name}_object",
                input_manager_key=table_input_manager_key
            )
        },
        metadata=config.metadata() if isinstance(config, SheetConfig) else None,
        description=config.description if isinstance(config, SheetConfig) else partition_freshness_policy,
        required_resource_keys={
            table_input_manager_key,
            table_engine_resource_key,
            table_copy_resource_key,
        },
        resource_defs=table_resource_defs,
        io_manager_def=table_io_manager_def,
        io_manager_key=table_io_manager_key,
        compute_kind="object_to_table",
        partitions_def=partitions_def,
        op_tags=op_tags | {"kind": "s3_to_redshift"} if op_tags is not None else {"kind": "s3_to_redshift"},
        group_name=group_name,
        output_required=output_required,
        freshness_policy=config.object_freshness_policy if isinstance(config, SheetConfig) else None,
        retry_policy=retry_policy,
        code_version=_compute_code_version(config)
    )
    def _table_asset(
            context: OpExecutionContext,
            data: Dict[str, str]
    ) -> Output[Dict[str, pd.DataFrame]]:
        gsheet_copy_resource: ConfigurableQueryCopyResource = getattr(context.resources, table_copy_resource_key)
        table_engine_resource = getattr(context.resources, table_engine_resource_key)
        if isinstance(config, SheetConfig):
            uri = data[next(iter(data))]
            target_table = context.assets_def.asset_key.path[0]
            query = _gsheet_copy_query(target_table=target_table, uri=uri)
            result = table_engine_resource.execute_query(
                query=gsheet_copy_resource.build_query(query=query),
                fetch_results=True
            )[0]
            tables = {"default": pd.DataFrame({
                "schema_name": gsheet_copy_resource.schema_name,
                "table_name": target_table
            }, index=[0], columns=["schema_name", "table_name"])}
            metadata = dict(
                rows=int(result[0]),
                mb=result[1],
                schema=gsheet_copy_resource.schema_name,
                table=target_table
            )
        else:
            by_partition: Dict[str, pd.DataFrame] = {}

            for partition_key in context.asset_partition_keys_for_output():
                uri = data[partition_key]
                target_table = partition_key
                query = _gsheet_copy_query(target_table=target_table, uri=uri)
                result = table_engine_resource.execute_query(
                    query=gsheet_copy_resource.build_query(query=query),
                    fetch_results=True
                )[0]
                by_partition[partition_key] = pd.DataFrame({
                    "schema_name": gsheet_copy_resource.schema_name,
                    "table_name": target_table
                }, index=[0], columns=["schema_name", "table_name"])
            metadata = {}
            tables = by_partition
        return Output(
            value=tables,
            # https://github.com/dagster-io/dagster/issues/12498
            metadata=metadata,
        )

    return [_object_asset, _table_asset]


def build_assets(
        parent_name: str,
        configs: Mapping[str, SheetConfig],
        partitioned: bool = False,
        object_io_manager_key: Optional[str] = None,
        object_io_manager_def: Optional[IOManagerDefinition] = None,
        table_io_manager_key: Optional[str] = None,
        table_io_manager_def: Optional[IOManagerDefinition] = None,
        table_resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        op_tags: Optional[Mapping[str, Any]] = None,
        output_required: bool = True,
        retry_policy: Optional[RetryPolicy] = None,
        table_input_manager_key: str = "gsheet_table_input_loader",
        table_engine_resource_key: str = "redshift",
        table_copy_resource_key: str = "gsheet_query",
) -> Sequence[AssetsDefinition]:
    if partitioned:
        partition_defs = StaticPartitionsDefinition([name for name in configs])
        return _build_assets(
            name=parent_name,
            group_name=parent_name,
            config=configs,
            object_io_manager_key=object_io_manager_key,
            object_io_manager_def=object_io_manager_def,
            table_io_manager_key=table_io_manager_key,
            table_io_manager_def=table_io_manager_def,
            table_resource_defs=table_resource_defs,
            partitions_def=partition_defs,
            op_tags=op_tags,
            output_required=output_required,
            retry_policy=retry_policy,
            table_input_manager_key=table_input_manager_key,
            table_engine_resource_key=table_engine_resource_key,
            table_copy_resource_key=table_copy_resource_key,
        )
    return [
        asset
        for name in configs
        for asset in _build_assets(
            name=name,
            config=configs[name],
            object_io_manager_key=object_io_manager_key,
            object_io_manager_def=object_io_manager_def,
            table_io_manager_key=table_io_manager_key,
            table_io_manager_def=table_io_manager_def,
            table_resource_defs=table_resource_defs,
            op_tags=op_tags,
            group_name=parent_name,
            output_required=output_required,
            retry_policy=retry_policy,
            table_input_manager_key=table_input_manager_key,
            table_engine_resource_key=table_engine_resource_key,
            table_copy_resource_key=table_copy_resource_key,
        )
    ]
