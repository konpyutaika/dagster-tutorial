import time
from typing import Mapping

from ...libraries.dagster_gsheet.gsheet_resources import (
    SheetConfig,
)

from ...libraries.dagster_gsheet.assets_factory import build_assets

configs: Mapping[str, SheetConfig] = {
    "hiring_plan_metrics": SheetConfig(
        spreadsheet_id="1KN70AzGPZYtlFVb_EbLDn1qH3dij2rK0Rt0Jcei2CiI",
        sheet_name="hiring_plan_metrics",
        columns_range="A:D"
    ),
    "officevibe_data": SheetConfig(
        spreadsheet_id="1KN70AzGPZYtlFVb_EbLDn1qH3dij2rK0Rt0Jcei2CiI",
        sheet_name="officevibe_data",
        columns_range="A:D"
    )
}
assets = build_assets(
    parent_name="gsheet",
    configs=configs,
    object_io_manager_key="s3_pd_csv_io_manager",
    table_io_manager_key="s3_pd_csv_io_manager",
    table_input_manager_key="s3_path_input_loader",
    partitioned=True,
)
