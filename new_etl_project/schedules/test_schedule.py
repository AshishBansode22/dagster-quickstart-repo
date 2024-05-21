from dagster import ScheduleDefinition
from new_etl_project.jobs.test_job import asset_job


everyday_12noon = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 12 * * *",
    execution_timezone="US/Pacific",
)