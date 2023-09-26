from dagster import load_assets_from_package_module

from . import cats
from . import gsheets
from . import redshift

CATS = "cats"
GSHEETS = "gsheets"
REDSHIFT = "redshift"

cats_assets = load_assets_from_package_module(package_module=cats, group_name=CATS)
gsheet_assets = load_assets_from_package_module(package_module=gsheets)
redshift_assets = load_assets_from_package_module(package_module=redshift, group_name=REDSHIFT)
