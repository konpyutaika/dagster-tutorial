from dagster import load_assets_from_package_module

from . import cats

CATS = "cats"

cats_assets = load_assets_from_package_module(package_module=cats, group_name=CATS)
