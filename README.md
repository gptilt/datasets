# Datasets

Open datasets for the community!

Normalized, curated and enriched, these datasets were *specifically* designed for data science workloads.

You can find the published datasets in [GPTilt's Hugging Face profile](https://huggingface.co/gptilt). All datasets are published in parquet format.

Alternatively, if you are interested in running the data pipelines yourself, find instructions [below](#getting-started).

## Basic

Datasets are split into two tiers:

- `basic`: Basic datasets are true to the raw data, with some additional transformations that improve coherence (usage of `matchId` instead of `gameId`), increase usability (e.g. the addition of inventory data to the `events` table in the `matches` dataset), reduce the scope (fields that aren't particularly relevant are removed), and denormalize (e.g. splitting kill events into kill and assist events) the underlying data.
- `ultimate`: Ultimate datasets, on the other hand, perform opinionated transformations, with some specific goal in mind. Data accuracy remains, but the dataset structure is markedly different.

### Matches

The match dataset contains all data available in the Riot API for a given set of matches.
Currently, the dataset can be found in the following variants:

- [`10k` Challenger matches](https://huggingface.co/datasets/gptilt/lol-basic-matches-challenger-10k), includes over 15M events from ranked matches with at least one challenger player. The 10 largest regions are included.
- *SOON* `100k` Challenger matches, includes over 150M events.

## Ultimate

### Events

The `ultimate` tier `events` dataset contains enriched events from a selection of matches from the Riot Games API. This dataset was specifically designed for time series analysis, with all events being timestamped and providing improved (compared to the `basic` tier `events` dataset) contextual information about every event.

Currently, the dataset can be found in the following variants:

- [Over `10M` events from `10K` Challenger matches](https://huggingface.co/datasets/gptilt/lol-ultimate-events-challenger-10m), includes over 10M events from ranked matches with at least one challenger player. Events are enriched with pregame context (player champions), and up-to-date game state context (inventories, corrected levels). The 10 largest regions are included.

### Snapshot

The `ultimate` tier `snapshot` dataset contains snapshots from a selection of matches from the Riot Games API. This dataset was specifically designed for isolated estimation/classification, namely win probability estimation, with all the relevant game state information included.

Currently, the dataset can be found in the following variants:

- [Snapshots at 15 minutes events from `10K` Challenger matches](https://huggingface.co/datasets/gptilt/lol-ultimate-snapshot-challenger-15min). Events were enriched with pregame context (player champions), and up-to-date game state context (inventories, corrected levels), then a snapshot at 15 minutes was taken. The 10 largest regions are included.

## Getting Started

If you'd rather do things yourself, you must clone the repository, open a terminal inside the newly created directory, create and activate a Python virtual environment, and run `pip install .` inside the cloned repository.

Then, you'll be able to use the CLI.

### `ds-cdragon`

Gets data from the [CommunityDragon](https://communitydragon.org/) CDN, namely, coordinates for all structures and jungle camps.

### `ds-riot-api`

Requires the parameters `--key` and `--root`, that specify the Riot API Key, and the storage root directory, respectively.

Example usage:

```bash
ds-riot-api \
  --root $DATASET_ROOT \
  --key $RIOT_API_KEY
```

If no production key is available, the entire ETL process for building the datasets is network bound. The code is not particularly optimized for performance, because the limiting factor is the API rate limits.

A thread is spawned per platform, to ensure asyncio tasks from different platforms are managed independently, because rate limits are applied on a per-region basis.
From each thread, every API call is ran as a coroutine.

### `ds-tables`

Runs ETL pipelines for the specified table. Requires specifying a schema first (e.g. `basic`). Admits the flag option `--flush`, which forces it to write to disk even if a table's partition is smaller than 1GB.

Example usage:

```bash
ds-tables basic matches \
  --root $DATASET_ROOT \
  --flush --count 10000
```

### Useful Commands

#### Count number of ranked matches in raw

```bash
grep -rnw "${DATASET_ROOT}/raw/riot_api/match_info" -e 'queueId": 420' | wc -l
```

## Contributing

Contributions are welcome! If you have ideas for new utilities, find bugs, or want to improve existing code, please feel free to open an issue or submit a pull request on the GitHub repository.

## License

All datasets are licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).

GPTilt isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.
