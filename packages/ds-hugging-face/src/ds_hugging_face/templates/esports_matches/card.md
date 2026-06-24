---
{{ card_data }}
---

# GPTilt: {{ pretty_name }}

This dataset is part of the [GPTilt](https://github.com/gptilt) open-source initiative, aimed at democratizing access to high-quality LoL data for research and analysis, fostering public exploration, and advancing the community's understanding of League of Legends through data science and AI. It provides a clean, canonical record of the competitive matches and games of professional League of Legends.

*By using this dataset, users accept full responsibility for any consequences arising from its use. GPTilt assumes no liability for any damages that may result. Users are strongly encouraged to review the ["Uses"](#uses) section—particularly the ["Out-of-Scope Use"](#out-of-scope-use) subsection—for guidance.*

## Getting Started

First, install Hugging Face's [datasets](https://pypi.org/project/datasets/) package:

```bash
pip install datasets
```

Now, you can load the dataset!

```py
from datasets import load_dataset

# Specify just the config_name / table
dataset = load_dataset("gptilt/{{ id }}", name="{{ example_table }}")

# Or include the split!
dataset = load_dataset("gptilt/{{ id }}", name="{{ example_table }}", split="train")
```

## Dataset Summary

{{ dataset_summary }} {{ purpose }}

## Dataset Structure

The data is structured into {{ num_tables }} tables, each exposed as a separate config:

{% include tables %}

Each table is a single, unpartitioned snapshot (one `train` split per config).

## Dataset Creation

### Curation Rationale

The structured record of a professional game — which patch it was played on, the champions picked and banned, the two teams, and the result — is scattered across match histories and broadcasts. This dataset was created to provide a stable, canonical record of competitive **games**, **matches** (series), and **tournaments**, so that competitive results can be analyzed at scale and so that broadcast/VOD commentary can be linked back to the specific game it discusses (the patch and champion compositions being the anchor).

### Source Data

#### Data Collection and Processing

The source data originates from [**Leaguepedia**](https://lol.fandom.com/) (the League of Legends esports wiki), accessed through its structured [**Cargo**](https://lol.fandom.com/wiki/Special:CargoTables) query API.

1. **Extraction:** The `ScoreboardGames`, `MatchSchedule`, and `Tournaments` Cargo tables are queried in full on a weekly schedule.
2. **Raw Storage:** Raw Cargo responses (JSON) are snapshotted to object storage, partitioned by week.
{% include transformations %}
4. **Output:** Each clean table is written to Parquet and published here as a separate config.

#### Who are the source data producers?

The underlying data is authored by the **Leaguepedia community** of volunteer editors, documenting competitive League of Legends matches and tournaments. The dataset curators are the contributors to the GPTilt project, who perform the automated collection, cleaning, and projection steps described above.

### Bias, Risks, and Limitations

- **Coverage Bias:** Leaguepedia's depth varies by region and competitive tier. Major regions and tier-1 competition are documented far more completely than minor regions and lower tiers.
- **Snapshot-Only:** The clean layer is **overwrite-only** — it reflects the *latest* state of Leaguepedia at collection time, with no history retained.
- **Editorial Lag & Gaps:** As a community wiki, Leaguepedia may lag behind, and older games in particular may have incomplete picks/bans or missing identifiers.

#### Recommendations

- Treat the data as a **best-effort snapshot** of a community wiki, not an authoritative or complete record.
- Expect missing fields for older games; guard for null picks/bans and identifiers.
- Acknowledge the **coverage bias** toward major regions/tiers when reporting aggregate results.

## Uses

### Disclaimer

*This dataset is derived from [Leaguepedia](https://lol.fandom.com/), a Fandom-hosted community wiki. GPTilt is not endorsed by Fandom, Leaguepedia, or Riot Games, and does not reflect their views or opinions. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.*

### License

This dataset is licensed under **`{{ license }}`**. Because it is derived from Leaguepedia content, redistribution and derivative works must provide **attribution to Leaguepedia** and remain under compatible **share-alike** terms. Every row carries a `source_url` linking back to the originating wiki page to support attribution.

### Direct Use

This dataset is intended for **non-commercial research, data analysis, and tooling** aimed at understanding and organizing competitive League of Legends data. Suitable uses include:

- **Competitive analysis** — draft/meta trends, win rates, objective control, and results across patches and regions.
- **Grounding** — linking VOD/broadcast commentary to the specific game it discusses, via patch + champion composition.
- **Educational purposes** related to data science and esports analytics.

### Out-of-Scope Use

This dataset is a best-effort community snapshot and **must not** be relied upon as an authoritative record, nor used in any way that violates Leaguepedia's [licensing terms](https://lol.fandom.com/wiki/License).

{% include changelist %}

## Citation

**If you wish to use this dataset in your work, we kindly ask that you cite it.**

For most informal work, a simple mention of the GPTilt project and the {{ pretty_name }} dataset will suffice. Please also **attribute Leaguepedia** as the upstream source.

**BibTeX:**

```bibtex
@misc{gptilt_{{ pretty_name | replace(" ", "_") | lower }},
  author    = { {{ curators | default("GPTilt Contributors", true) }} },
  title     = { {{ pretty_name }} },
  year      = { {{ creation_date }} },
  publisher = { Hugging Face },
  journal   = { Hugging Face Hub },
  url       = { https://huggingface.co/datasets/gptilt/{{ id }} }
}
```
