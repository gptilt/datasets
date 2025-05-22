## Changelist

### May 22, 2025

- Account for system-assigned items, such as the support item assignment on game start.
- Remove unnecessary fields from `matches` table.

### May 18, 2025

- Challenge and mission information were removed from the `m̀atches` table.
- `ELITE_MONSTER_KILL` and `CHAMPION_KILL` events were split into `_KILL` and `_ASSIST` events, respectively.
- `CHAMPION_KILL` events were split into `CHAMPION_KILL` and `CHAMPION_KILLED` events.
- Event field `killerId` was replaced by `participantId`, with the exception of the new `CHAMPION_KILLED` events.
- Normalize rune information in `participants`.
- Create `OBJECTIVE_BOUNTY_START` event from `OBJECTIVE_BOUNTY_PRESTART` event (announcement).