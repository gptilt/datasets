---
{{ card_data }}
---

# GPTilt: {{ pretty_name }}

This dataset is part of the [GPTilt](https://github.com/gptilt) open-source initiative, aimed at democratizing access to high-quality LoL data for research and analysis, fostering public exploration, and advancing the community's understanding of League of Legends through data science and AI. It provides a clean, canonical reference for the people and organizations of competitive League of Legends.

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

Esports data is scattered across match histories, broadcasts, and community sites, and the same person or team appears under many different names (in-game names, real names, romanizations, abbreviations, wiki redirects). This dataset was created to provide a stable, canonical directory of esports **public figures** and **teams**, together with a unified **entity alias index** that makes it possible to resolve those many names back to a single entity — a prerequisite for linking and normalizing esports data across sources.

### Source Data

#### Data Collection and Processing

The source data originates from [**Leaguepedia**](https://lol.fandom.com/) (the League of Legends esports wiki), accessed through its structured [**Cargo**](https://lol.fandom.com/wiki/Special:CargoTables) query API.

1. **Extraction:** The `Players`, `PlayerRedirects`, and `Teams` Cargo tables are queried in full on a weekly schedule.
2. **Raw Storage:** Raw Cargo responses (JSON) are snapshotted to object storage, partitioned by week.
{% include transformations %}
4. **Output:** Each clean table is written to Parquet and published here as a separate config.

#### Who are the source data producers?

The underlying data is authored by the **Leaguepedia community** of volunteer editors, documenting the careers of competitive League of Legends players and the histories of esports organizations. The dataset curators are the contributors to the GPTilt project, who perform the automated collection, cleaning, and unification steps described above.

#### Personal and Sensitive Information

**This dataset describes real, identifiable people** — professional players, coaches, analysts, casters, and team staff — and includes **real names, in-game names, nationalities/regions, and career status**. All of this information is sourced from **Leaguepedia, where it is already published** about individuals in their **public, professional capacity** as competitors and public figures.

No private contact information (emails, addresses, phone numbers) is collected. Users must handle this data responsibly and in accordance with Leaguepedia's [licensing and privacy terms](https://lol.fandom.com/wiki/License) and applicable data-protection law. **This data must not be used to harass, dox, profile, or otherwise harm the individuals it describes.** See the ["Out-of-Scope Use"](#out-of-scope-use) section.

### Bias, Risks, and Limitations

- **Coverage Bias:** Leaguepedia's depth varies by region and competitive tier. Major regions and tier-1 competition are documented far more completely than minor regions and lower tiers, so the directory is not an even sample of all esports participants.
- **Snapshot-Only:** The clean layer is **overwrite-only** — it reflects the *latest* state of Leaguepedia at collection time, with no history retained. Fields like `active_from`/`active_to` are placeholders here and will be populated in a future `curated` (SCD Type 2) layer.
- **Editorial Lag:** As a community wiki, Leaguepedia may lag behind real-world roster changes, retirements, or rebrands, and may contain occasional errors or gaps.
- **Name Ambiguity:** A single surface name can map to more than one entity (e.g. a recycled in-game name). The `entity_aliases` index keeps every mapping and discriminates by `entity_type`, but consumers must expect one-to-many matches.

#### Recommendations

- Treat the data as a **best-effort snapshot** of a community wiki, not an authoritative or complete registry.
- When matching on `entity_aliases`, **disambiguate using `entity_type`** and handle multiple candidate entities for the same string.
- Acknowledge the **coverage bias** toward major regions/tiers when reporting aggregate results.

## Uses

### Disclaimer

*This dataset is derived from [Leaguepedia](https://lol.fandom.com/), a Fandom-hosted community wiki. GPTilt is not endorsed by Fandom, Leaguepedia, or Riot Games, and does not reflect their views or opinions. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.*

### License

This dataset is licensed under **`{{ license }}`**. Because it is derived from Leaguepedia content, redistribution and derivative works must provide **attribution to Leaguepedia** and remain under compatible **share-alike** terms. Every row carries `source` and `source_url` columns linking back to the originating wiki page to support attribution.

### Direct Use

This dataset is intended for **non-commercial research, data analysis, and tooling** aimed at understanding and organizing competitive League of Legends data. Suitable uses include:

- **Entity resolution** — linking players and teams across match data, broadcasts, and other esports datasets.
- **Name normalization** — resolving in-game names, real names, romanizations, and abbreviations to a canonical entity.
- **Descriptive analysis** of the competitive landscape (regions, roles, organizations).
- **Educational purposes** related to data science and esports analytics.

### Out-of-Scope Use

This dataset **must not** be used to **harass, dox, surveil, profile, or otherwise harm** the individuals it describes, nor for any purpose that violates Leaguepedia's [licensing and privacy terms](https://lol.fandom.com/wiki/License) or applicable data-protection law. It is a best-effort community snapshot and **must not** be relied upon as an authoritative record for decisions affecting individuals.

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
