- **snapshot**: Contains a snapshot of the match at a given time, with contextual information such as kills/assists, as well as pregame state (champions, runes, etc).

```json
{
  "matchId": "LA2_1495348800",
  # Player information
  "kills_0": 6,
  "deaths_0": 2,
  "assists_0": 3,
  "inventory_0": [1421, 3500],  # Item IDs
  "level_0": 12,  # Level at time of event
  (...)
  "kills_1": 0,
  "deaths_1": 1,
}
```

All snapshots have a `matchId` column, making it compatible with all [`basic` tier `matches` tables](https://huggingface.co/datasets/gptilt/lol-basic-matches-challenger-10k) and [the `ultimate` tier `events` dataset](https://huggingface.co/datasets/gptilt/lol-ultimate-events-challenger-10m).
