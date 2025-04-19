import aiohttp
import asyncio
from storage import Storage, StorageParquet
from riot_api import get, schema, transform


async def region_worker(region: str, list_of_player_uuids: list[str]):
    print(f"[{region}] Region worker spawned.")
    storage_raw = Storage('riot-api', 'raw', region, ['match_info', 'match_timeline'])
    storage_stg = StorageParquet('riot-api', 'stg', region, tables=['matches', 'participants', 'events'])

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
        await asyncio.gather(*[
            process_match(match_id, session) for match_id in list_of_match_ids
        ])

    async def process_match(match_id, session):
        print(f"[{region}] Processing match {match_id}...")

        if storage_stg.has_records_in_all_tables(matchId=match_id):
            print(f"[{region}] Match {match_id} already exists.")
            return

        try:
            info = storage_raw.read_file('match_info', record=match_id)
            timeline = storage_raw.read_file('match_timeline', record=match_id)
            print(f"[{region}] Match {match_id} found in raw.")
        except FileNotFoundError:
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
            print(f"[{region}] Match {match_id} stored in raw.")

        match, participants = transform.match_into_match_and_participants(match_id=match_id, match=info)
        events = transform.timeline_into_events(timeline=timeline)

        storage_stg.store_batch('matches', [match])
        storage_stg.store_batch('participants', participants)
        storage_stg.store_batch('events', events, schema=schema.EVENTS)
        print(f"[{region}] Match {match_id} stored in buffer.")

    async with aiohttp.ClientSession() as session:
        # Fire all puuid tasks at once
        await asyncio.gather(*[process_puuid(puuid, session) for puuid in list_of_player_uuids])

    storage_stg.flush()
