from dagster import load_assets_from_package_module

from . import cats
from . import airflow_metadata
from . import tableau

CATS = "cats"
TABLEAU = "tableau"
AIRFLOW_METADATA = "airflow_metadata"

cats_assets = load_assets_from_package_module(package_module=cats, group_name=CATS)
airflow_metadata_assets = load_assets_from_package_module(package_module=airflow_metadata, group_name=AIRFLOW_METADATA)
tableau_assets = load_assets_from_package_module(package_module=tableau, group_name=TABLEAU)