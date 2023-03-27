from dagster import load_assets_from_package_module

from . import cats
from . import airflow_metadata

CATS = "cats"
AIRFLOW_METADATA = "airflow_metadata"

cats_assets = load_assets_from_package_module(package_module=cats, group_name=CATS)
airflow_metadata_assets = load_assets_from_package_module(package_module=airflow_metadata, group_name=AIRFLOW_METADATA)