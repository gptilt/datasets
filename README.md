# Datasets

Open datasets for the community!

Normalized, curated and enriched, these datasets were *specifically* designed for data science workloads.

You can find the published datasets in [GPTilt's Hugging Face profile](https://huggingface.co/gptilt). All datasets are published in parquet format, or available as Apache Iceberg tables.

Alternatively, if you are interested in running the data pipelines yourself, find instructions [below](#getting-started).

## The GPTilt Dataset Catalogue

> If you're looking for the previous dataset tiering, please refer to the [relevant doc](./docs/old_dataset_tiering.md).

The **GPTilt Dataset Catalogue** splits datasets into two tiers:

- **Clean:** these datasets are true to the raw data, with some additional transformations that improve coherence (usage of `matchId` instead of `gameId`), increase usability (e.g. the addition of inventory data), reduce the scope (fields that aren't particularly relevant are removed), and denormalize (e.g. splitting kill events into kill and assist events) the underlying data.
- **Curated:** these datasets, on the other hand, perform opinionated transformations, with some specific goal in mind. Data accuracy remains, but the dataset structure is markedly different. It can be aggregated at a different level of granularity, have additional columns based on complex rules, or one-hot encodings.

## Naming Convention

1. A table or storage directory's fully qualified name is `root.dataset.schema.table`.
2. The `root` is the environment (e.g. `dev`, `prod`).
3. The `dataset` is the highest logical aggregator - typically the platform from which the data was originally collected (e.g. `riot_api`, `youtube`).
4. The `schema` specifies the degree of quality of the data (e.g. `raw`, `clean`, `curated`).
5. The `table` specifies a relation (e.g. `league_entries`, which contains data collected from the Riot API on player entries in a league).

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
