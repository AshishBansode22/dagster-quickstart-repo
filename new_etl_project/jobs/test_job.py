from dagster import (
    define_asset_job,
    AssetSelection
)

# define jobs as selections over the larger graph
asset_job = define_asset_job("test_job", AssetSelection.groups("etl_jobs"))