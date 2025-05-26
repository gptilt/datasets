## Changelist

### May 26, 2025

- Refactored inventory generation, splitting it into two output columns: `inventoryIds` and `inventoryCounts`, containing item IDs and their respective counts, respectively. Fixed the inventory algorithm to handle `ITEM_UNDO` events correctly. Both columns are padded to a maximum length of 8, making them easier to work with (e.g. when performing column explosion in `pandas`/`polars`).

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