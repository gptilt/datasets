- **`public_figures`**: One row per esports public figure (pro players, ex-pros, coaches, analysts, casters, owners). Identity is `person_id`, the canonical Leaguepedia page key. Includes `canonical_name`, `display_name`, `role`, `region`, retirement status, and the source wiki URL.

- **`teams`**: One row per esports organization/team, keyed by `team_id` (the canonical Leaguepedia page key). Includes `canonical_name`, `region`, `location`, and a disbanded flag.

- **`entity_aliases`**: A unified alias index. Every known surface name maps to a canonical entity via `entity_id`, with:
  - `entity_type` — `person` or `team`, distinguishing which directory the `entity_id` points at.
  - `alias_type` — where the name came from: `ign` (in-game name), `real_name`, `romanization`, `other` (wiki redirects/alt-names) for people; `short` (name/abbreviation) for teams.

All three tables share `source` and `source_url` columns linking each row back to its Leaguepedia page. To resolve a name to an entity, match on `entity_aliases.alias`, read `entity_type`/`entity_id`, then join to `public_figures.person_id` or `teams.team_id` accordingly.
