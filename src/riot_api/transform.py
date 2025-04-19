import datetime as dt
import pyarrow as pa


def match_into_match_and_participants(
    match_id: str,
    match: dict,
) -> tuple[dict, list[dict]]:
    # match['info'] only has 'gameId'
    match['info']['matchId'] = match_id
    for participant in match['info']['participants']:
        participant['matchId'] = match_id

    participants = match['info'].pop('participants')

    return match['info'], participants


def timeline_into_events(
    timeline: dict,
) -> list[dict]:
    # Treat participant frames as events
    list_of_events = [
        (lambda d: d.update({
            'participantId': int(participant_id),
            'timestamp': frame['timestamp'],
            'type': 'PARTICIPANT_FRAME'
        }) or d)(participant_frame)
        for frame in timeline['info']['frames'] for participant_id, participant_frame in frame['participantFrames'].items()
    ]

    # Add events from timeline
    for frame in timeline['info']['frames']:
        list_of_events.extend(frame['events'])
    
    # Preprocess events and collect all unique keys
    all_columns = set()
    for event in list_of_events:
        event['matchId'] = timeline['metadata']['matchId']
        event.pop('gameId', None)
        
        if 'position' in event:
            event['positionX'] = event['position']['x']
            event['positionY'] = event['position']['y']
            event.pop('position', None)

        event.pop('realTimestamp', None)
        all_columns.update(event.keys())

    # Standardize events with missing keys set to None
    return [{col: event.get(col) for col in all_columns} for event in list_of_events]


def yearmonth_from_match(match_info: dict) -> str:
    datetime = dt.datetime.fromtimestamp(
        match_info['info']['gameCreation'] / 1000,
        tz=dt.timezone.utc
    )

    return f"{datetime.year}{datetime.strftime("%m")}"


def sanitize_row(row, schema):
    for field in schema:
        key = field.name
        if key not in row or row[key] is None:
            continue

        if pa.types.is_struct(field.type):
            row[key] = sanitize_row(row[key], field.type)
        elif pa.types.is_int16(field.type):
            try:
                row[key] = int(row[key])
            except Exception:
                pass
        elif pa.types.is_int32(field.type):
            try:
                row[key] = int(row[key])
            except Exception:
                pass
    return row
