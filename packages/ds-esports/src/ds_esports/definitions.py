"""
Dagster jobs and schedules for ds-esports.

Single weekly job covers raw → clean for Phase 1; both assets share
`partition_per_week`, so one partition_key fans out across the whole DAG.
"""
import dagster as dg

from .partitions import partition_per_week


# All assets in the package are tagged with group_name="esports" — selecting on
# the group keeps the job definition future-proof when we add matches/scoreboards.
job_esports_phase_1 = dg.define_asset_job(
    name="job_esports_phase_1",
    selection=dg.AssetSelection.groups("esports"),
    partitions_def=partition_per_week,
)


# Weekly Sunday 06:00 UTC
@dg.schedule(
    name="schedule_esports_phase_1",
    job=job_esports_phase_1,
    cron_schedule="0 6 * * 0",
    execution_timezone="UTC",
)
def schedule_esports_phase_1(context: dg.ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date().isoformat()
    return dg.RunRequest(run_key=day, partition_key=day)
