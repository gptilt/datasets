from dorans import death
import json
import re


def find_closest_event(
    event: dict,
    list_of_events: list[dict],
    **event_filters: dict,
) -> list[dict] | None:
    """
    Find the closest event in the list of events.
    Provided a dictionary of event filters,
    it will filter the events based on the filters.
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
    region: str,
) -> tuple[dict, list[dict]]:
    match_info = match['info']
    # match['info'] only has 'gameId'
    match_info['matchId'] = match_id
    # Add region
    match_info['region'] = region
    # Add server
    match_info['server'] = re.match(r"[a-zA-Z]+", match_id).group(0) or ""

    # Add ban information to participant
    team_info = {
        team['teamId']: {
            'bans': team['bans'],
            'feats': team['feats'],
            'objectives': team['objectives'],
            'win': team['win'],
        }
        for team in match_info.pop('teams')
    }

    # Normalize team data
    for team_id, team in team_info.items():
        team_prefix = f"team_{team_id}_"

        # Feats of Strength
        for feat_type, feat_state in team['feats'].items():
            match_info[f"{team_prefix}{feat_type}"] = feat_state
        
        # Objectives
        for objective_name, objective_data in team['objectives'].items():
            match_info[f"{team_prefix}{objective_name}_first"] = objective_data['first']
            match_info[f"{team_prefix}{objective_name}_kills"] = objective_data['kills']

        # Winner
        if team['win'] == True:
            match_info['winner_team_id'] = team_id

    # Participants is a list of records, and thus have the matchId column
    # to make each record unique
    for i, participant in enumerate(match_info['participants']):
        participant['matchId'] = match_id

        # Draft information
        _team = team_info[participant['teamId']]
        participant['ban'] = _team['bans'][i % 5]['championId']
        participant['pickTurn'] = _team['bans'][i % 5]['pickTurn']

        # Runes
        # Primary Runes
        for j, rune in enumerate(participant['perks']['styles'][0]['selections']):
            participant[f'rune_primary_{j}'] = rune['perk']
        # Secondary Runes
        for j, rune in enumerate(participant['perks']['styles'][1]['selections']):
            participant[f'rune_secondary_{j}'] = rune['perk']
        # Shards
        for shard_category, shard_id in participant['perks']['statPerks'].items():
            participant[f'rune_shard_{shard_category}'] = shard_id

        # Remove unnecessary data
        for key in [
            'PlayerScore0', 'PlayerScore1',
            'PlayerScore2', 'PlayerScore3',
            'PlayerScore4', 'PlayerScore5',
            'PlayerScore6', 'PlayerScore7',
            'PlayerScore8', 'PlayerScore9',
            'PlayerScore10', 'PlayerScore11',
            'playerAugment1', 'playerAugment2',
            'playerAugment3', 'playerAugment4',
            'playerAugment5', 'playerAugment6',
            'challenges', 'missions',
            'riotIdGameName', 'riotIdTagline',
        ]:
            participant.pop(key)

    # Split participants from match
    participants = match_info.pop('participants')

    return match_info, participants


def respawn_event_from_kill_event(
    event: dict,
    list_of_events: list[dict],
) -> dict:
    victim_level = find_closest_event(
        event,
        list_of_events,
        type='LEVEL_UP',
        participantId=event['victimId']
    ) or {'level': 1}

    death_timer = death.timer(
        level=victim_level['level'],
        game_minutes=event['timestamp'] / 60000,
    )

    return {
        'matchId': event['matchId'],
        'type': 'RESPAWN',
        'timestamp': event['timestamp'] + death_timer,
        'participantId': event['victimId'],
        'timeSpentDead': death_timer,
    }


def assist_events_from_kill_event(
    event: dict,
    assisting_players: list[int] | None = None,
) -> list[dict]:    
    list_of_assist_events = []

    for assist_id in assisting_players:
        assist_event = event.copy()
        assist_event['type'] = event['type'].replace('KILL', 'ASSIST')
        assist_event['participantId'] = assist_id

        list_of_assist_events.append(assist_event)

    return list_of_assist_events


def killed_event_from_kill_event(
    event: dict,
) -> dict:
    killed_event = event.copy()
    killed_event['type'] = 'CHAMPION_KILLED'
    killed_event['participantId'] = event['victimId']
    killed_event['killerId'] = event['participantId']
    return killed_event


def update_participant_inventory(
    inventory: list[int],
    event: dict,
) -> dict:
    """
    Update the inventory of a participant based on the event.
    """
    if event['type'] == 'ITEM_PURCHASED':
        inventory.append(event['itemId'])
    elif event['type'] in ('ITEM_SOLD', 'ITEM_DESTROYED'):
        if event['itemId'] in inventory:
            inventory.remove(event['itemId'])
    elif event['type'] == 'ITEM_UNDO':
        if event['afterId'] == 0 and event['beforeId'] in inventory:  # Undo purchase
            inventory.remove(event['beforeId'])
        else:  # Undo sell
            inventory.append(event['afterId'])
    
    return inventory


def objective_bounty_start_from_prestart(event: dict):
    return {
        "matchId": event["matchId"],
        "type": "OBJECTIVE_BOUNTY_START",
        "timestamp": event['actualStartTime'],
        "teamId": event["teamId"]
    }


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
    all_columns = set(["eventId"])
    # Keep track of inventories
    inventory = {participant['participantId']: [] for participant in participants}

    for event in list_of_events:
        event['matchId'] = timeline['metadata']['matchId']
        event.pop('gameId', None)
        
        if 'position' in event:
            event['positionX'] = event['position']['x']
            event['positionY'] = event['position']['y']
            event.pop('position', None)

        # Rename 'creatorId' and 'killerId' to 'participantId'
        event['participantId'] = (
            event.pop('creatorId', event.pop('killerId', event.get('participantId')))
        )
        # Only keep ingame timestamp
        event.pop('realTimestamp', None)
    
        # Add default position for item and dragon events
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
        # Add respawn event
        if event['type'] == 'CHAMPION_KILL':
            list_of_events.append(
                respawn_event_from_kill_event(event, list_of_events)
            )
        # Add numberOfAssists to all _KILL events
        if event["type"].endswith("_KILL"):
            event['numberOfAssists'] = len(event.get('assistingParticipantIds', []))
        # Split KILL events into KILL and ASSIST
        if 'assistingParticipantIds' in event:
            list_of_events.extend(assist_events_from_kill_event(
                event,
                event.pop("assistingParticipantIds")
            ))
        # Split CHAMPION_KILL events into KILL and KILLED
        if event["type"] == "CHAMPION_KILL":
            list_of_events.append(
                killed_event_from_kill_event(event)
            )
        # Add inventory to ITEM_ events
        if event['type'].startswith('ITEM_'):
            event['inventory'] = update_participant_inventory(
                inventory[event["participantId"]],
                event,
            ).copy()

        # Add objective bounty start event from announcement
        if event['type'] == 'OBJECTIVE_BOUNTY_PRESTART':
            list_of_events.append(
                objective_bounty_start_from_prestart(event)
            )
        
        all_columns.update(event.keys())

    # Standardize events with missing keys set to None
    return [{
        col: event.get(col) if col != "eventId" else event_id
        for col in all_columns
    } for event_id, event in enumerate(list_of_events) ]
