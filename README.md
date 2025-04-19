# Datasets

Open datasets for the community!

## License

All datasets are licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).

© Riot Games, data collected from the Riot Games API. GPTilt isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

## Riot API

If no production key is available, the entire ETL process for building the datasets is network bound. The code is not particularly optimized for performance, because the limiting factor is the API rate limits.

A thread is spawned per platform, to ensure asyncio tasks from different platforms are managed independently, because rate limits are applied on a per-region basis.
From each thread, every API call is ran as a coroutine.

### Tables

This dataset consists of the following tables:

- `match_metadata`: contains metadata regarding a match.
- `match_participants`: links a match's `participantIds` to the player's `PUUID`.
- `match_events`: contains all events from matches. Includes a custom `PARTICIPANT_FRAME` event type, which models the participant data at any frame (taken at `frameInterval` - which, in the public Riot API, is a minute). Additionally, all `position` fields in all events have been split into two unique fields `positionX` and `positionY`.

All match tables have a `matchId` column, making it possible to join tables with data from different regions without conflict (the `gameId` column, on the other hand, is not unique across regions).
