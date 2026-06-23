# Datasets

Open datasets for the community!

Normalized, curated and enriched, these datasets were *specifically* designed for data science workloads.

You can find the published datasets in [GPTilt's Hugging Face profile](https://huggingface.co/gptilt). All datasets are published in parquet format, or available as Apache Iceberg tables.

Alternatively, if you are interested in running the data pipelines yourself, find instructions [below](#getting-started).

## The GPTilt Dataset Catalogue

> If you're looking for the previous dataset tiering, please refer to the [relevant doc](./docs/old_dataset_tiering.md).

The **GPTilt Dataset Catalogue** lists the datasets we publish — what's available, **where** to get it, and **how** it's licensed.

Published datasets are split into two quality tiers:

- **Clean:** these datasets are true to the raw data, with some additional transformations that improve coherence (usage of `matchId` instead of `gameId`), increase usability (e.g. the addition of inventory data), reduce the scope (fields that aren't particularly relevant are removed), and denormalize (e.g. splitting kill events into kill and assist events) the underlying data.
- **Curated:** these datasets, on the other hand, perform opinionated transformations, with some specific goal in mind. Data accuracy remains, but the dataset structure is markedly different. It can be aggregated at a different level of granularity, have additional columns based on complex rules, or one-hot encodings.

| Dataset | Tier | Tables | Available on | Access |
| --- | --- | --- | --- | --- |
| [`gptilt/lol-esports-entities`](https://huggingface.co/datasets/gptilt/lol-esports-entities) | Clean | `public_figures`, `teams`, `entity_aliases` | 🤗 Hugging Face | Free · CC BY-SA 3.0 |

> Distribution via [AWS Data Exchange](https://aws.amazon.com/data-exchange/) is planned; rows in that column will be filled in as datasets are listed there. All currently published data is free to access — licensing terms are per-dataset and always restated on the Hugging Face card.

## Internal Catalogue

How each source dataset is structured and which tables it produces, across schema tiers. Not every table here is published externally — see the [GPTilt Dataset Catalogue](#the-gptilt-dataset-catalogue) for what is.

### Naming Convention

1. A table or storage directory's fully qualified name is `root.dataset.schema.table`.
2. The `root` is the environment (e.g. `dev`, `prod`).
3. The `dataset` is the highest logical aggregator - typically the platform from which the data was originally collected (e.g. `riot_api`, `leaguepedia`).
4. The `schema` specifies the degree of quality of the data (e.g. `raw`, `clean`, `curated`).
5. The `table` specifies a relation (e.g. `league_entries`, which contains data collected from the Riot API on player entries in a league).

> The `dataset` segment names the **source platform** the data came from, never the consuming app or the game domain. Leaguepedia data lives under `leaguepedia` (not `esports` or `lol`), the same way Riot's data lives under `riot_api` — so a future source (e.g. a different wiki for another game) becomes its own self-contained namespace rather than being crammed into a shared one. The broader "esports" grouping lives at the orchestration layer (the `ds-esports` package / Dagster asset group), not in storage.

The tables below are written as `dataset.schema.table`; the `root` (`dev`/`prod`) is omitted.

### `riot_api` — [Riot Games API](https://developer.riotgames.com/apis)

| Schema | Table | Storage | Description |
| --- | --- | --- | --- |
| `raw` | `league_entries` | S3 (parquet) | Raw league-entry snapshots per scrape. |
| `clean` | `fact_player_rank` | Iceberg | One row per player rank observation. |

### `leaguepedia` — [Leaguepedia](https://lol.fandom.com/) (via Cargo)

| Schema | Table | Storage | Description |
| --- | --- | --- | --- |
| `raw` | `players` | S3 (JSON) | Raw Cargo `Players` rows, append-only per scrape. |
| `raw` | `player_redirects` | S3 (JSON) | Raw Cargo `PlayerRedirects` rows (name → canonical page). |
| `raw` | `teams` | S3 (JSON) | Raw Cargo `Teams` rows, append-only per scrape. |
| `clean` | `public_figures` | Iceberg | One row per esports public figure, keyed by `person_id`. |
| `clean` | `teams` | Iceberg | One row per organization/team, keyed by `team_id`. |
| `clean` | `entity_aliases` | Iceberg | Unified alias index mapping every surface name to its canonical entity. |

### `document` — Enriched document store

| Schema | Table | Storage | Description |
| --- | --- | --- | --- |
| `stg` | `transcripts` | S3 (parquet) | Staged transcripts feeding document enrichment. |

## Getting Started

If you'd rather do things yourself, the easiest way to go about it is to clone the repository, open a terminal inside the newly created directory, and run `make init`. This will create the Python virtual environment and install the project dependencies. You can then activate the virtual environment with `source venv/bin/activate` if you're in a Linux environment, or `source venv/Scripts/activate` if you're in a Windows environment.

Then, you'll be able to use the Dagster CLI to boot the orchestrator up:

```sh
dagster dev
```

> The ingestion, processing, and curation of the GPTilt Dataset Catalogue is orchestrated with [Dagster](https://dagster.io). We highly recommend you get acquainted with Dagster before moving forward.

Most pipelines require a number of secrets that should be available at runtime as environment variables. If you include them in a `.env` file in the repository root, Dagster will automatically load them before executing the pipelines. They are:

- `HUGGING_FACE_TOKEN`: a Hugging Face token with **write** access to the `gptilt` organization's dataset repositories (publishing creates repos and uploads data + cards);
- `RIOT_API_KEY`: a Riot Games API key - any kind should work;
- `FANDOM_USERNAME`: a Leaguepedia (Fandom) bot login, in the form `Account@BotName`, used by `ds-esports` to query Cargo;
- `FANDOM_PASSWORD`: the password for the Leaguepedia bot login;
- `S3_BUCKET_NAME`: the name of the S3-compatible bucket;
- `S3_BUCKET_ENDPOINT`: the name of the S3-compatible bucket;
- `S3_BUCKET_ACCESS_KEY_ID`: the name of the S3-compatible bucket;
- `S3_BUCKET_SECRET_ACCESS_KEY`: the name of the S3-compatible bucket;
- `CATALOG_ENDPOINT`: the endpoint of the Iceberg catalog;
- `CATALOG_WAREHOUSE_NAME`: the warehouse name of the Iceberg catalog;
- `CATALOG_TOKEN`: the token for accessing the Iceberg catalog.

> ⚠️ Not all pipelines are public, but the repository utilities (e.g. `make init`) take that into account - so everything should work fine!.

### Modules

- `ds-chatbot`: One of the very few private repositories of GPTilt! 🤫
- `ds-common`: Common utilities for the repository. 🛠️
- `ds-documents`: For building an enriched document store. 📚
- `ds-esports`: For building assets from [Leaguepedia](https://lol.fandom.com/) — esports public figures, teams, and a unified alias index. 🏆
- `ds-hugging-face`: For publishing datasets to [🤗 Hugging Face](https://huggingface.co/gptilt).
- `ds-riot-api`: For building assets with provenance from the [Riot Games API](https://developer.riotgames.com/apis). 🌐
- `ds-scribe`: For transcribing audio to text. 🖊️
- `ds-storage`: Custom storage interface. Maintenance limited to usage. 🏭
- `ds-tables`: TBD.

## Contributing

Contributions are welcome! If you have ideas for new utilities, find bugs, or want to improve existing code, please feel free to open an issue or submit a pull request on the GitHub repository.

## License

Each dataset is licensed according to its upstream source — the applicable license is always declared on the dataset's Hugging Face card:

- **Riot Games API** datasets are licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).
- **Leaguepedia** (`leaguepedia`) datasets are licensed under the [Creative Commons Attribution-ShareAlike 3.0 License (CC BY-SA 3.0)](https://creativecommons.org/licenses/by-sa/3.0/), matching Leaguepedia's own terms (attribution + share-alike).

GPTilt isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

The `leaguepedia` datasets are derived from [Leaguepedia](https://lol.fandom.com/), a Fandom-hosted community wiki; GPTilt isn't endorsed by Fandom or Leaguepedia, and that content remains under CC BY-SA, attributable to its contributors.
