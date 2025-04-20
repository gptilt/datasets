# Datasets

Open datasets for the community!

Normalized, curated and enriched, these datasets were *specifically* designed for data science workloads.

You can find the published datasets in [GPTilt's Hugging Face profile](https://huggingface.co/gptilt). All datasets are published in parquet format.

Alternatively, if you are interested in running the data pipelines yourself, find instructions [below](#getting-started).

## Match

The match dataset contains all data available in the Riot API for a given set of matches.
Currently, the dataset can be found in the following sizes:

- [`10k` Challenger matches](https://huggingface.co/datasets/gptilt/riot-match-challenger-10k), includes over 10M events from ranked matches with at least one challenger player. The 10 largest regions are included.
- *SOON* `100k` Challenger matches, includes over 100M events from ranked matches with at least one challenger player. The 10 largest regions are included.

This dataset consists of the following tables:

- `match_metadata`: contains metadata regarding a match.
- `match_participants`: links a match's `participantIds` to the player's `PUUID`, and includes all the player endgame information regarding a match.
- `match_events`: contains all events from matches. Includes a custom `PARTICIPANT_FRAME` event type, which models the participant data at any frame (taken at `frameInterval` - which, in the public Riot API, is a minute). Additionally, all `position` fields in all events have been split into two unique fields `positionX` and `positionY`. Finally, a default position is added for item events (the respective team's spawn coordinates) and `DRAGON_SOUL_GIVEN` events (the dragon pit's coordinates).

All match tables have a `matchId` column, making it possible to join tables with data from different regions without conflict (the `gameId` column, on the other hand, is not unique across regions).

## License

All datasets are licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).

GPTilt isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

## Getting Started

If you'd rather do things yourself, you must clone the repository, open a terminal inside the newly created directory, create and activate a Python virtual environment, and run `pip install .` inside the cloned repository.

Then, you'll be able to use the following commands:

- `ds-cdragon`: Gets data from the [CommunityDragon](https://communitydragon.org/) CDN, namely, coordinates for all structures and jungle camps. This will be needed to enrich the [Match dataset](#match).
- `ds-riot-api`: Has two subcommands, `raw` and `stg`. Both require the parameters `--key` and `--root`, that specify the Riot API Key, and the storage root directory, respectively. Additionally, `stg` also admits the flag option `--flush`, which forces it to write to disk even if a table's partition is smaller than 1GB.

### Riot API

If no production key is available, the entire ETL process for building the datasets is network bound. The code is not particularly optimized for performance, because the limiting factor is the API rate limits.

A thread is spawned per platform, to ensure asyncio tasks from different platforms are managed independently, because rate limits are applied on a per-region basis.
From each thread, every API call is ran as a coroutine.
