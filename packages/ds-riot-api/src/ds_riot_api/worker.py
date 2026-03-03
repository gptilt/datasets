from ds_common import print, tqdm_range
from datetime import datetime as dt
import random


async def match(
    region: str,
    regions_and_platforms: dict[str, set[str]],
    root: str,
):
    storage_raw = src.Storage(
        root,
        'raw',
        'riot_api',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    print(f"[{region}] Region worker spawned.")

    async def process_puuid(puuid, session):
        try:
            list_of_match_ids = storage_raw.read_files('player_match_ids', record=puuid, region=region)
        except FileNotFoundError:
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

        list_of_players = []

        # Retrieve player uuids
        print(f"Fetching player uuids from Riot API...")
        for platform in regions_and_platforms[region]:
            players = await get.fetch_with_rate_limit('players', session=session, platform=platform)
            list_of_players.extend(players["entries"])

        print(f"[{region}] Found {len(list_of_players)} player uuids.")

        random.shuffle(list_of_players)
        for i in tqdm_range(len(list_of_players), desc=region):
            await process_puuid(list_of_players[i]["puuid"], session)



async def mastery(
    region: str,
    regions_and_platforms: dict[str, set[str]],
    root: str,
):
    storage_raw = src.Storage(
        root,
        'raw',
        'riot_api',
        ['player_match_ids', 'match_info', 'match_timeline', 'player_champion_mastery']
    )
    print(f"[{region}] Region worker spawned.")
    
    list_of_players = [
        p.stem  # find_files() returns a list of PosixPath's
        for p in storage_raw.find_files(
            'player_match_ids',
            '*',
            count=-1,
            region=region
        )
    ]
    print(f"[{region}] Found {len(list_of_players)} player uuids.")
    print(list_of_players)

    async def process_puuid(puuid, session):
        print(f"[{region}] Processing player {puuid}...")

        print(f"[{region}] Fetching champion mastery for {puuid}...")
        platform = await get.fetch_with_rate_limit(
            'player_region', session=session, region=region, puuid=puuid
        )
        mastery = await get.fetch_with_rate_limit(
            'player_champion_mastery', session=session, platform=platform['region'], puuid=puuid
        )
        
        storage_raw.store_file(
            'player_champion_mastery',
            puuid,
            mastery,
            region=region,
            yearmonthday=dt.now().strftime('%Y%m%d')
        )
    
    async with aiohttp.ClientSession() as session:
        random.shuffle(list_of_players)
        for i in tqdm_range(len(list_of_players), desc=region):
            await process_puuid(list_of_players[i], session)
