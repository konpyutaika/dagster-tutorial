from typing import Dict

import pandas as pd
from dagster import asset, Output, FreshnessPolicy, AssetIn, SpecificPartitionsPartitionMapping, OpExecutionContext

from ...libraries.dagster_gsheet.configurable_query_resources import ConfigurableQueryResource


@asset(
    compute_kind="redshift",
    required_resource_keys={"redshift_query", "redshift"},
    freshness_policy=FreshnessPolicy(cron_schedule="2-59/5 * * * *", maximum_lag_minutes=5)
)
def data_for_people_kpis_5_min(
        context: OpExecutionContext,
        hiring_plan_metrics: Dict[str, pd.DataFrame],
        officevibe_data: Dict[str, pd.DataFrame],
) -> Output[Dict[str, str]]:
    target_table = context.assets_def.asset_key.path[0]
    hiring_plan_metrics = hiring_plan_metrics[next(iter(hiring_plan_metrics))].to_dict(orient='index')[0]
    officevibe_data = officevibe_data[next(iter(officevibe_data))].to_dict(orient='index')[0]

    query = f"""
    SET query_group TO fast_track;
    SET statement_timeout to 86400000;

    CREATE TABLE IF NOT EXISTS "{{schema_name}}"."{target_table}" (
        total_filled_positions INT,
        average_enps FLOAT,
        updated_at TIMESTAMP
    );
    DELETE FROM "{{schema_name}}"."{target_table}" WHERE updated_at = '2022-01-01';
    INSERT INTO "{{schema_name}}"."{target_table}" (updated_at, total_filled_positions, average_enps)
    SELECT '2022-01-01' as updated_at, avg(enps) as average_enps, sum(filled_positions) as total_filled_positions
    FROM "{hiring_plan_metrics['schema_name']}"."{hiring_plan_metrics['table_name']}", "{officevibe_data['schema_name']}"."{officevibe_data['table_name']}"

    """

    redshift_resource = context.resources.redshift
    query_builder_resource: ConfigurableQueryResource = context.resources.redshift_query
    redshift_resource.execute_query(query=query_builder_resource.build_query(query))

    return Output(
        value=dict(table=target_table, schema=query_builder_resource.schema_name),
        metadata=dict(
            schema=query_builder_resource.schema_name,
            table=target_table
        )
    )


@asset(
    compute_kind="redshift",
    required_resource_keys={"redshift_query", "redshift"},
    freshness_policy=FreshnessPolicy(cron_schedule="30 * * * *", maximum_lag_minutes=60)
)
def data_for_people_kpis_hourly(
        context: OpExecutionContext,
        hiring_plan_metrics: Dict[str, pd.DataFrame],
        officevibe_data: Dict[str, pd.DataFrame],
) -> Output[Dict[str, str]]:
    target_table = context.assets_def.asset_key.path[0]
    hiring_plan_metrics = hiring_plan_metrics[next(iter(hiring_plan_metrics))].to_dict(orient='index')[0]
    officevibe_data = officevibe_data[next(iter(officevibe_data))].to_dict(orient='index')[0]

    query = f"""
    SET query_group TO fast_track;
    SET statement_timeout to 86400000;

    CREATE TABLE IF NOT EXISTS "{{schema_name}}"."{target_table}" (
        total_filled_positions INT,
        average_enps FLOAT,
        updated_at TIMESTAMP
    );
    DELETE FROM "{{schema_name}}"."{target_table}" WHERE updated_at = '2022-01-01';
    INSERT INTO "{{schema_name}}"."{target_table}" (updated_at, total_filled_positions, average_enps)
    SELECT '2022-01-01' as updated_at, avg(enps) as average_enps, sum(filled_positions) as total_filled_positions
    FROM "{hiring_plan_metrics['schema_name']}"."{hiring_plan_metrics['table_name']}", "{officevibe_data['schema_name']}"."{officevibe_data['table_name']}"

    """

    redshift_resource = context.resources.redshift
    query_builder_resource: ConfigurableQueryResource = context.resources.redshift_query
    redshift_resource.execute_query(query=query_builder_resource.build_query(query))

    return Output(
        value=dict(table=target_table, schema=query_builder_resource.schema_name),
        metadata=dict(
            schema=query_builder_resource.schema_name,
            table=target_table
        )
    )
