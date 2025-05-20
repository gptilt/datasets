from . import schema, transform
from common import print, tqdm_range
import storage


def main(
    region: str,
    root: str,
    count: int = 1000,
    flush: bool = True,
    overwrite: bool = False,
):
    storage_raw = storage.Storage(
        root,
        'raw',
        'riot_api',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    storage_basic = storage.StoragePartition(
        root,
        'basic',
        'matches',
        tables=['matches', 'participants', 'events'],
        region=region
    )

    def process_match(match_id: str) -> tuple[dict, list[dict], list[dict]]:
        print(f"[{region}] Processing match {match_id}...")

        if not overwrite and storage_basic.has_records_in_all_tables(matchId=match_id):
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

        match, participants = transform.match_into_match_and_participants(
            match_id=match_id,
            match=info,
            region=region,
        )
        events = transform.timeline_into_events(timeline=timeline, participants=participants)

        return match, participants, events
        
    list_of_match_ids = storage_raw.find_files(
        'match_info',
        record='*',
        region=region,
        count=int(count * 1.5)
    )

    matches, participants, events = [], [], []

    real_count = 0
    for i in tqdm_range(list_of_match_ids, desc=region):
        if real_count == count:
            continue
        data = process_match(list_of_match_ids[i].name.split('/')[-1].split('.json')[0])
        if data:
            matches.append(data[0])
            participants.extend(data[1])
            events.extend(data[2])
            real_count += 1
         
    print(f"[{region}] Storing {len(matches)} matches...")
    storage_basic.store_batch('matches', matches)
    storage_basic.store_batch('participants', participants)
    storage_basic.store_batch('events', events, schema=schema.EVENTS)

    if flush:
        storage_basic.flush(
            {'events': schema.EVENTS},
        )
