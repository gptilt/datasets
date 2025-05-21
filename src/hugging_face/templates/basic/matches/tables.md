- **matches**: Contains match-level metadata (e.g., `matchId`, `gameDuration`, `gameVersion`, `winningTeam`).

```json
{
  "matchId": "LA2_1495348800",
  "region": "americas",
  "server": "LA",
  "gameStartTimestamp": 1743465021436,
  "team_100_atakhan_first": true,
  "team_100_atakhan_kills": 1,
  (...)
}
```

- **participants**: Links a match's `participantIds` to the player's `PUUID`, and includes all the player endgame information regarding a match. It contains details for each of the 10 participants per match (e.g., `puuid`, `championId`, `teamId`, final stats like kills, deaths, assists, gold earned, items).

```json
{
  "matchId": "LA2_1495348800",
  "participantId": 10,  # Red team support
  "teamId": 200,
  "teamPosition": "TOP",
  "championId": 43,
  "championName": "Karma",
  "physicalDamageDealt": 6075,
  (...)
}
```

- **events**: Contains a detailed timeline of in-game events (e.g., `CHAMPION_KILL`, `ITEM_PURCHASED`, `WARD_PLACED`, `BUILDING_KILL`, `ELITE_MONSTER_KILL`) with timestamps, positions (where applicable), involved participants/items, etc. Additionally, to facilitate analysis:
  - All `position` fields in all events have been split into two unique fields `positionX` and `positionY`.
  - Periodic snapshots (taken at `frameInterval` - in the public Riot API, every minute) of all participant states (`participantFrames`) are split into custom per-participant `PARTICIPANT_FRAME` events.
  - `ELITE_MONSTER_KILL` and `CHAMPION_KILL` events are split into `_KILL` and `_ASSIST` events, with one event per participant.
  - `CHAMPION_KILL` events are split into `CHAMPION_KILL` and `CHAMPION_KILLED` events, respectively. This helps model the game as a series of events that happen/are enacted to/by individual participants in the game.
  - A default position is added for item events (the respective team's spawn coordinates - when the player is playing the champion Ornn, his latest coordinates are used instead) and `DRAGON_SOUL_GIVEN` events (the dragon pit's coordinates).

```json
{
  "matchId": "LA2_1495348800",
  "eventId": 10,  # IDs are attributed per match
  "timestamp": 194787,
  "type": "LEVEL_UP",
  (...)
}
```

All match tables have a `matchId` column, making it possible to join tables with data from different regions without conflict (the `gameId` column, on the other hand, is not unique across regions).
