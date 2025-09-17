import dagster as dg
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud
from riot_api import *


defs = dg.Definitions(
    assets=link_code_references_to_git_if_cloud(assets_defs=dg.with_source_code_references([
        asset_raw_riot_api_league_entries_per_day_per_server_x_tier_x_division,
        asset_clean_riot_api_league_entries_per_day_per_server_x_tier_x_division,
        asset_riot_api_player_matches_per_day_per_server,
    ])),
    jobs=[
        job_riot_api_league_entries,
        job_riot_api_player_matches,
    ],
    schedules=[
        schedule_riot_api_league_entries,
        schedule_riot_api_player_matches
    ],
    resources={},
)