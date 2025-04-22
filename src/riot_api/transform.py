import datetime as dt
import json


def find_closest_event(
    event: dict,
    list_of_events: list[dict],
    **event_filters: dict,
) -> dict | None:
    """
    Find the closest event in the list of events.
    """
    filtered_events = [
        e for e in list_of_events
        if all(e[key] == value for key, value in event_filters.items())
    ]

    # Sort events by timestamp difference
    filtered_events.sort(key=lambda x: abs(x['timestamp'] - event['timestamp']))

    return filtered_events[0] if filtered_events else None


def default_position_from_event_type(
    event: dict,
    champion_name: str,
    list_of_events: list[dict],
) -> tuple[int, int]:
    """
    Default position for events that don't have a position.
    """
    if event["type"] == "DRAGON_SOUL_GIVEN":
        with open('data/cdragon/coordinates/camps.json', 'r') as f:
            camp_coordinates = json.load(f)
        return camp_coordinates.get('Dragon', (None, None))
    
    # If the event is an item event and the champion is Ornn,
    # use Ornn's nearest known coordinates
    if event["type"].startswith("ITEM_") and champion_name == "Ornn":
        closest_ornn_frame = find_closest_event(
            event,
            list_of_events,
            type='PARTICIPANT_FRAME',
            participant_id=event.get('participantId')
        )
        return closest_ornn_frame['positionX'], closest_ornn_frame['positionY']

    # Else, use spawn coordinates for item events
    spawn_key = (
        "OrderSpawnGate" if event['participantId'] <= 5 else "ChaosSpawnGate"
    )
    with open('data/cdragon/coordinates/buildings.json', 'r') as f:
        building_coordinates = json.load(f)

    return building_coordinates.get(spawn_key, (None, None))


def match_into_match_and_participants(
    match_id: str,
    match: dict,
) -> tuple[dict, list[dict]]:
    # match['info'] only has 'gameId'
    match['info']['matchId'] = match_id

    # Participants is a list of records, and thus have the matchId column
    # to make each record unique
    for participant in match['info']['participants']:
        participant['matchId'] = match_id
    # Pop before returning, to not store it with match['info'] as well
    participants = match['info'].pop('participants')

    return match['info'], participants


def timeline_into_events(
    timeline: dict,
    participants: list[dict],
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

        # Ward creators are participants
        if 'creatorId' in event:
            event['participantId'] = event.pop('creatorId')

        event.pop('realTimestamp', None)
        all_columns.update(event.keys())
    
    # Add default position for item and dragon events
    for event in list_of_events:
        if event['type'].startswith('ITEM_') or event['type'] == 'DRAGON_SOUL_GIVEN':
            event['positionX'], event['positionY'] = default_position_from_event_type(
                event,
                champion_name=[
                    participant['championName']
                    for participant in participants
                    if participant['participantId'] == event.get('participantId')
                ] if 'participantId' in event else None,
                list_of_events=list_of_events
            )

    # Standardize events with missing keys set to None
    return [{col: event.get(col) for col in all_columns} for event in list_of_events]


def yearmonth_from_match(match_info: dict) -> str:
    datetime = dt.datetime.fromtimestamp(
        match_info['info']['gameCreation'] / 1000,
        tz=dt.timezone.utc
    )

    return f"{datetime.year}{datetime.strftime("%m")}"
