- **`games`**: One row per competitive game, keyed by `game_id` (Leaguepedia `GameId`). Includes the **patch**, the blue/red teams (`team_blue` = Team1, `team_red` = Team2), `winner`, the champion **picks** and **bans** per side (lists of champion names, role-ordered top→jungle→mid→bot→support for picks), `game_number` within its series, the parent `match_id` and `tournament`, `datetime_utc`, `gamelength`, the Riot platform game id (`riot_game_id`), and a `vod` link.

- **`tournaments`**: One row per tournament, keyed by `tournament_id` (the canonical Leaguepedia page key). Includes `name`, `league`, `region`, `split`, `year`, `tier`, and `date_start`/`date_end`.

- **`matches`**: One row per match — a *series* (e.g. Bo3/Bo5) — keyed by `match_id`. Includes the two teams, the series score (`team1_score`/`team2_score`), `winner` (`1` | `2`), `best_of`, `tab`, the parent `tournament`, and `datetime_utc`.

Join keys: `games.match_id` → `matches.match_id`; `games.tournament` and `matches.tournament` → `tournaments.tournament_id`. Team and champion names resolve to canonical entities via the companion [lol-esports-entities](https://huggingface.co/datasets/gptilt/lol-esports-entities) `entity_aliases` index.
