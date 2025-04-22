import aiohttp
import asyncio
import random
from storage import Storage, StorageParquet
from riot_api import get, schema, transform


async def raw(
    region: str,
    platforms: set[str],
    storage_raw: Storage,
):
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
        for platform in platforms:
            players = await get.fetch_with_rate_limit('players', session=session, platform=platform)
            list_of_players.extend(players["entries"])
            
        print(f"[{region}] Found {len(list_of_players)} player uuids.")

        random.shuffle(list_of_players)
        for player in list_of_players:
            await process_puuid(player["puuid"], session)


def stg(
    region: str,
    storage_raw: Storage,
    storage_stg: StorageParquet,
    flush: bool = True
):
    def process_match(match_id):
        print(f"[{region}] Processing match {match_id}...")

        if storage_stg.has_records_in_all_tables(matchId=match_id):
            print(f"[{region}] Match {match_id} already exists.")
            return
        
        try:
            info = storage_raw.read_files('match_info', record=match_id, region=region)
        except FileNotFoundError:
            print(f"[{region}] Match {match_id} not found in raw storage.")
            return
        
        if info["info"]["queueId"] != 420:
            print(f"[{region}] Match {match_id} is not ranked.")
            return
        timeline = storage_raw.read_files('match_timeline', record=match_id, region=region)

        match, participants = transform.match_into_match_and_participants(match_id=match_id, match=info)
        events = transform.timeline_into_events(timeline=timeline, participants=participants)

        print(f"[{region}] Storing {match_id}...")
        storage_stg.store_batch('matches', [match], region=region)
        storage_stg.store_batch('participants', participants, region=region)
        storage_stg.store_batch('events', events, schema=schema.EVENTS, region=region)
    
    list_of_match_ids = storage_raw.find_files(
        'match_info',
        record='*',
        region=region,
        count=1000
    )

    for match_id in list_of_match_ids:
        process_match(match_id)

    if flush:
        storage_stg.flush(region=region)
