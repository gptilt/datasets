## Changelist

### Jun 2026

- Initial release: `public_figures`, `teams`, and a unified `entity_aliases` index, sourced from Leaguepedia via its Cargo query API.
- Change: include the entity's canonical id as a resolvable alias - this makes it so every entity now appears in `entity_aliases`, even if it does not have additional aliases.
