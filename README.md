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

If you'd rather do things yourself, the easiest way to go about it is to clone the repository, open a terminal inside the newly created directory, and run `make init`. This will create the Python virtual environment and install the project dependencies. You can then activate the virtual environment with `source venv/bin/activate` if you're in a Linux environment, or `source venv/Scripts/activate` if you're in a Windows environment.

Then, you'll be able to use the Dagster CLI to boot the orchestrator up:

```sh
dagster dev
```

> The ingestion, processing, and curation of the GPTilt Dataset Catalogue is orchestrated with [Dagster](https://dagster.io). We highly recommend you get acquainted with Dagster before moving forward.

Most pipelines require a number of secrets that should be available at runtime as environment variables. If you include them in a `.env` file in the repository root, Dagster will automatically load them before executing the pipelines. They are:

- `HF_TOKEN`: a Hugging Face token with the following permissions:
  - `Read access to contents of all public gated repos you can access`;
- `RIOT_API_KEY`: a Riot Games API key - any kind should work;
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
- `ds-hugging-face`: For publishing datasets to [🤗 Hugging Face](https://huggingface.co/gptilt).
- `ds-riot-api`: For building assets with provenance from the [Riot Games API](https://developer.riotgames.com/apis). 🌐
- `ds-runtime`: Runtime variables and environment context. ⏲️
- `ds-scribe`: For transcribing audio to text. 🖊️
- `ds-storage`: Custom storage interface. Maintenance limited to usage. 🏭
- `ds-tables`: TBD.

## Contributing

Contributions are welcome! If you have ideas for new utilities, find bugs, or want to improve existing code, please feel free to open an issue or submit a pull request on the GitHub repository.

## License

All datasets are licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).

GPTilt isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.
