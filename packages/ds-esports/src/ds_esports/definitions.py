"""
Dagster jobs and schedules for ds-esports.

Single biweekly job covers both raw and clean for Phase 1 — the dependency graph runs
raw first, then clean, both within the same partition window. Jobs are unpartitioned
because raw assets self-discover their scrape window via `_scrape_partition_kwargs()`.
"""
import dagster as dg


# All assets in the package are tagged with group_name="esports" — selecting on
# the group keeps the job definition future-proof when we add matches/scoreboards.
job_esports_phase_1 = dg.define_asset_job(
    name="job_esports_phase_1",
    selection=dg.AssetSelection.groups("esports"),
)


# Weekly Mon 06:00 UTC. Plan doc calls for biweekly, but Leaguepedia data is small
# and the politeness budget is fine — running weekly costs us nothing and keeps the
# alias table fresher for the privacy detector.
schedule_esports_phase_1 = dg.ScheduleDefinition(
    name="schedule_esports_phase_1",
    job=job_esports_phase_1,
    cron_schedule="0 6 * * 1",
    execution_timezone="UTC",
)
