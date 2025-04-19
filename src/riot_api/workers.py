import aiohttp
import asyncio
from storage import Storage, StorageParquet
from riot_api import get, schema, transform


async def raw(
    region: str,
    list_of_player_uuids: list[str],
    storage_raw: Storage,
):
    print(f"[{region}] Region worker spawned.")
    semaphore = asyncio.Semaphore(3)  # Tune this to control per-region concurrency

    async def process_puuid(puuid, session):
        async with semaphore:
            list_of_match_ids = await get.fetch_with_rate_limit(
                'player_match_ids',
                session=session,
                region=region,
                puuid=puuid,
                count=50
            )
            storage_raw.store_file('player_match_ids', puuid, list_of_match_ids)

        await asyncio.gather(*[
            process_match(match_id, session) for match_id in list_of_match_ids
        ])

    async def process_match(match_id, session):
        print(f"[{region}] Processing match {match_id}...")

        async with semaphore:  # Rate limit bound
            print(f"[{region}] Fetching {match_id}...")
            info = await get.fetch_with_rate_limit(
                'match_info', session=session, region=region, match_id=match_id
            )
            timeline = await get.fetch_with_rate_limit(
                'match_timeline', session=session, region=region, match_id=match_id
            )
                
        storage_raw.store_file('match_info', match_id, info, yearmonth=transform.yearmonth_from_match(info))
        storage_raw.store_file('match_timeline', match_id, timeline, yearmonth=transform.yearmonth_from_match(info))

    async with aiohttp.ClientSession() as session:
        # Fire all puuid tasks at once
        await asyncio.gather(*[process_puuid(puuid, session) for puuid in list_of_player_uuids])


def stg(
    region: str,
    list_of_player_uuids: list[str],
    storage_raw: Storage,
    storage_stg: StorageParquet,
    flush: bool = True
):    
    def process_puuid(puuid: str):
        list_of_match_ids = storage_raw.read_file('player_match_ids', record=puuid)
        for match_id in list_of_match_ids:
            process_match(match_id) 

    def process_match(match_id):
        print(f"[{region}] Processing match {match_id}...")

        if storage_stg.has_records_in_all_tables(matchId=match_id):
            print(f"[{region}] Match {match_id} already exists.")
            return
        
        info = storage_raw.read_file('match_info', record=match_id)
        timeline = storage_raw.read_file('match_timeline', record=match_id)

        match, participants = transform.match_into_match_and_participants(match_id=match_id, match=info)
        events = transform.timeline_into_events(timeline=timeline)

        print(f"[{region}] Storing {match_id}...")
        storage_stg.store_batch('matches', [match], region=region)
        storage_stg.store_batch('participants', participants, region=region)
        storage_stg.store_batch('events', events, schema=schema.EVENTS, region=region)
    
    for puuid in list_of_player_uuids:
        process_puuid(puuid)

    if flush:
        storage_stg.flush(region=region)
