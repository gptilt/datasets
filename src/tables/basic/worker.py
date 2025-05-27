from .matches import schema, transform
from common import print, work_generator
import storage



def process_match(
    match_id: str,
    region: str,
    overwrite: bool,
    storage_raw: storage.Storage,
    storage_basic: storage.StoragePartition
) -> tuple[dict, list[dict], list[dict]]:
    # print(f"[{region}] Processing match {match_id}...")

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
    if info["info"]["participants"][0]["gameEndedInEarlySurrender"] == True:
        print(f"[{region}] Match {match_id} ended in early surrender.")
        return
    timeline = storage_raw.read_files('match_timeline', record=match_id, region=region)

    match, participants = transform.match_into_match_and_participants(
        match_id=match_id,
        match=info,
        region=region,
    )
    events = transform.timeline_into_events(timeline=timeline, participants=participants)

    return [match], participants, events


def main(
    region: str,
    root: str,
    dataset: str,
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
        dataset,
        tables=['matches', 'participants', 'events'],
        table_schema={"matches": schema.MATCHES, "events": schema.EVENTS},
        partition_col="region",
        partition_val=region,
    )
    
    list_of_match_ids = [
        filename.name.split('/')[-1].split('.json')[0]
        for filename in storage_raw.find_files(
            'match_info',
            record='*',
            region=region,
            count=int(count * 1.5)
    )]

    matches, participants, events = [], [], []
    for i, data in enumerate(work_generator(
        list_of_match_ids,
        process_match,
        descriptor=region,
        max_count=count,
        # kwargs
        overwrite=overwrite,
        storage_raw=storage_raw,
        storage_basic=storage_basic
    )):
        matches.extend(data[0])
        participants.extend(data[1])
        events.extend(data[2])

        if i % 500 == 0:
            print(f"[{region}] Storing batch...")
            storage_basic.store_batch('matches', matches)
            storage_basic.store_batch('participants', participants)
            storage_basic.store_batch('events', events)
            matches.clear()
            participants.clear()
            events.clear()

    print(f"[{region}] Storing batch...")
    storage_basic.store_batch('matches', matches)
    storage_basic.store_batch('participants', participants)
    storage_basic.store_batch('events', events)

    if flush:
        storage_basic.flush()
