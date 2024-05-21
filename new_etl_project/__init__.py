from dagster import Definitions, load_assets_from_modules

from . import assets_sample
from .io import io_file
from .jobs.test_job import asset_job
from .schedules.test_schedule import everyday_12noon

all_assets = load_assets_from_modules([assets_sample])

defs = Definitions(
    assets=all_assets,
    jobs=[asset_job],
    schedules=[everyday_12noon],
    resources={
        "io_file" : io_file.LocalFileSystemIOManager(),
    }
)
