from dagster import (
    AssetKey,
    EventLogEntry,
    RunRequest,
    RunConfig,
    SensorEvaluationContext,
    asset_sensor,
)
from new_etl_project.jobs.test_job import asset_job

# Define the sensor
@asset_sensor(asset_key=AssetKey("get_regionCodes"), job=asset_job)
def demo_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    yield RunRequest(
        run_key=context.cursor,
    )
