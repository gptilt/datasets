- **events**: Contains a **detailed timeline** of enriched in-game events (e.g., `CHAMPION_KILL`, `ITEM_PURCHASED`, `WARD_PLACED`, `BUILDING_KILL`, `ELITE_MONSTER_KILL`) with timestamps, and contextual information regarding both game state (player positions (where applicable), inventories, and levels) and pregame state (champions, runes, etc).

```json
{
  "matchId": "LA2_1495348800",
  "eventId": 10,  # IDs are attributed per match
  "timestamp": 194787,
  "type": "LEVEL_UP",
  # Player information
  "inventory_0": [1421, 3500],  # Item IDs
  "level_0": 12,  # Level at time of event
  (...)
  "inventory_1": [1258, 3500],
  "level_1": 11,
}
```

All events have a `matchId` column, making it compatible with all [`basic` tier `matches` tables](https://huggingface.co/datasets/gptilt/lol-basic-matches-challenger-10k).
