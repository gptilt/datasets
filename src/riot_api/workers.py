import aiohttp
import random
from riot_api import get, schema, transform
import storage
from tqdm import tqdm


def tqdm_range(
    iterable,
    desc: str = "",
    category_width: int = 12,
):
    return tqdm(range(len(iterable)), desc=f"[{desc}]".ljust(category_width) + "Processing matches")


async def raw(
    region: str,
    platforms: set[str],
    root: str,
):
    storage_raw = storage.Storage(
        root,
        'riot-api',
        'raw',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    tqdm.write(f"[{region}] Region worker spawned.")

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
        tqdm.write(f"[{region}] Processing match {match_id}...")

        tqdm.write(f"[{region}] Fetching {match_id}...")
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
        tqdm.write(f"Fetching player uuids from Riot API...")
        for platform in platforms:
            players = await get.fetch_with_rate_limit('players', session=session, platform=platform)
            list_of_players.extend(players["entries"])

        tqdm.write(f"[{region}] Found {len(list_of_players)} player uuids.")

        random.shuffle(list_of_players)
        for i in tqdm_range(list_of_players, desc=region):
            await process_puuid(list_of_players[i]["puuid"], session)


def stg(
    region: str,
    root: str,
    count: int = 1000,
    flush: bool = True,
):
    storage_raw = storage.Storage(
        root,
        'riot-api',
        'raw',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    storage_stg = storage.StorageParquet(
        root,
        'riot-api',
        'stg',
        tables=['matches', 'participants', 'events']
    )

    def process_match(match_id: str) -> tuple[dict, list[dict], list[dict]]:
        tqdm.write(f"[{region}] Processing match {match_id}...")

        if storage_stg.has_records_in_all_tables(matchId=match_id):
            tqdm.write(f"[{region}] Match {match_id} already exists.")
            return
        
        try:
            info = storage_raw.read_files('match_info', record=match_id, region=region)
        except FileNotFoundError:
            tqdm.write(f"[{region}] Match {match_id} not found in raw storage.")
            return
        
        if info["info"]["queueId"] != 420:
            tqdm.write(f"[{region}] Match {match_id} is not ranked.")
            return
        timeline = storage_raw.read_files('match_timeline', record=match_id, region=region)

        match, participants = transform.match_into_match_and_participants(match_id=match_id, match=info)
        events = transform.timeline_into_events(timeline=timeline, participants=participants)

        return match, participants, events
        
    list_of_match_ids = storage_raw.find_files(
        'match_info',
        record='*',
        region=region,
        count=count
    )

    matches, participants, events = [], [], []

    for i in tqdm_range(list_of_match_ids, desc=region):
        data = process_match(list_of_match_ids[i].name.split('/')[-1].split('.json')[0])
        if data:
            matches.append(data[0])
            participants.extend(data[1])
            events.extend(data[2])
         
    tqdm.write(f"[{region}] Storing {len(matches)} matches...")
    storage_stg.store_batch('matches', matches, region=region)
    storage_stg.store_batch('participants', participants, region=region)
    storage_stg.store_batch('events', events, schema=schema.EVENTS, region=region)

    if flush:
        storage_stg.flush(region=region)
