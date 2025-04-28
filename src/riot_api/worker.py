import aiohttp
from common import print, tqdm_range
import random
from riot_api import get, transform
import storage


async def raw(
    region: str,
    regions_and_platforms: dict[str, set[str]],
    root: str,
):
    storage_raw = storage.Storage(
        root,
        'riot-api',
        'raw',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    print(f"[{region}] Region worker spawned.")

    async def process_puuid(puuid, session):
        try:
            list_of_match_ids = storage_raw.read_files('player_match_ids', record=puuid, region=region)
        except FileNotFoundError:
            list_of_match_ids = await get.fetch_with_rate_limit(
                'player_match_ids',
                session=session,
                region=region,
                puuid=puuid,
                queue=420,
                type='ranked',
                count=50
            )
            storage_raw.store_file(
                'player_match_ids',
                puuid,
                list_of_match_ids,
                region=region,
            )
        
        for match_id in list_of_match_ids:
            try:
                storage_raw.find_files_in_tables(
                    match_id,
                    ['match_info', 'match_timeline'],
                    region=region
                )
            except FileNotFoundError:
                await process_match(match_id, session) 

    async def process_match(match_id, session):
        print(f"[{region}] Processing match {match_id}...")

        print(f"[{region}] Fetching {match_id}...")
        info = await get.fetch_with_rate_limit(
            'match_info', session=session, region=region, match_id=match_id
        )
        timeline = await get.fetch_with_rate_limit(
            'match_timeline', session=session, region=region, match_id=match_id
        )
                
        storage_raw.store_file(
            'match_info',
            match_id,
            info,
            region=region,
            yearmonth=transform.yearmonth_from_match(info)
        )
        storage_raw.store_file(
            'match_timeline',
            match_id,
            timeline,
            region=region,
            yearmonth=transform.yearmonth_from_match(info)
        )

    async with aiohttp.ClientSession() as session:
        list_of_players = []

        # Retrieve player uuids
        print(f"Fetching player uuids from Riot API...")
        for platform in regions_and_platforms[region]:
            players = await get.fetch_with_rate_limit('players', session=session, platform=platform)
            list_of_players.extend(players["entries"])

        print(f"[{region}] Found {len(list_of_players)} player uuids.")

        random.shuffle(list_of_players)
        for i in tqdm_range(list_of_players, desc=region):
            await process_puuid(list_of_players[i]["puuid"], session)
