from src import storage
from src.riot_api import transform

raw = storage.Storage(
    "/mnt/ai/gptilt/datasets",
    'riot-api',
    'raw',
    ['player_match_ids', 'match_info', 'match_timeline']
)
stg = storage.StorageParquet(
    "/mnt/ai/gptilt/datasets",
    'riot-api',
    'stg',
    tables=['matches', 'participants', 'events']
)

list_of_timelines = raw.read_files('match_timeline', record='*', count=1000, region='europe')
unique_events = {}
event_count = 0
for match in list_of_timelines:
    events = transform.timeline_into_events([], timeline=match)
    for event in events:
        event_count += 1
        unique_events[event["type"]] = event

import json
with open('output/unique_events.json', 'w') as f:
    json.dump(unique_events, f, indent=4)

print(f"Total events: {event_count}")
