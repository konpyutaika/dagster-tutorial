
from dagster import build_asset_reconciliation_sensor, AssetSelection

redshift_sensor = build_asset_reconciliation_sensor(
    asset_selection=AssetSelection.groups("redshift", "gsheet"),
    name="redshift_reconciliation_sensor",
)
