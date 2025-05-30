---
{{ card_data }}
---

# GPTilt: {{ pretty_name }}

This dataset is part of the [GPTilt](https://github.com/gptilt) open-source initiative, aimed at democratizing access to high-quality LoL data for research and analysis, fostering public exploration, and advancing the community's understanding of League of Legends through data science and AI. It provides detailed data from high-elo matches.

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
dataset = load_dataset("gptilt/{{ id }}", name="{{ example_table }}", split="{{ example_split }}")
```

## Dataset Summary

This dataset contains **{{ pretty_name }}**. {{ purpose }} Data was originally collected and processed via the official Riot Games API. It's {{ monolinguality }}, with the primary language being {{ primary_language }}.

## Dataset Structure

The data is structured into {{ num_tables }} tables:

{% include tables %}

Additionally, data is segmented into {{ num_splits }} splits: {{ splits }}.

## Dataset Creation

### Curation Rationale

This dataset was created to address the lack of large-scale, publicly available, and analysis-ready datasets for League of Legends research. The GPTilt project aims to provide resources for the community to apply data science and AI techniques to better understand the intricate dynamics of the game, moving beyond simple win prediction towards interpreting strategic patterns and complex interactions. This specific dataset focuses on high-elo (Challenger) players to capture refined strategic execution.

### Source Data

#### Data Collection and Processing

The source data originates exclusively from the [**Riot Games API**](https://developer.riotgames.com/apis) and [**CDragon**](https://communitydragon.org/).

1. **Seeding:** High-elo player PUUIDs were initially identified using the `league-v4` endpoint for the Challenger tier across multiple regions.
2. **Match History:** The `match-v5` endpoint was used to retrieve recent match IDs for these players.
3. **Match & Timeline Fetching:** The `match-v5` (match details) and `match-v5` (match timeline) endpoints were used to download the full data for each unique match ID identified.
4. **Raw Storage:** Raw API responses (JSON format) were saved.
{% include transformations %}
6. **Output:** Data was written to Parquet files, partitioned by `region`.

#### Who are the source data producers?

The underlying gameplay data is generated by **League of Legends players** participating in high-elo ranked matches. The **Riot Games API** serves as the source interface providing access to this gameplay data. The dataset curators are the contributors to the GPTilt project who performed the collection and processing steps. No demographic information about the players is collected, besides the region.

#### Personal and Sensitive Information

The dataset contains **PUUIDs** and **Participant IDs**, which are pseudonymous identifiers linked to League of Legends accounts. No other Personally Identifiable Information (PII) like real names, emails, or addresses is included. Use of these identifiers is subject to Riot Games' policies. Users should exercise caution and adhere to these policies, avoiding attempts to [deanonymize players who cannot reasonably be identified from visible information](https://developer.riotgames.com/policies/general#_developer-safety).

### Bias, Risks, and Limitations

- **Skill Tier Bias:** This dataset focuses *exclusively* on the Challenger tier. Findings may not generalize to other skill levels (Bronze, Silver, Gold, Platinum, Diamond, Master, Grandmaster) where metas, champion picks, and strategic execution differ significantly. Because match data is selected by searching for Challenger players, multi-tier games may (and are expected) to be present in the dataset.
- **Regional Bias:** While collected from multiple regions, the distribution might not be perfectly balanced, potentially reflecting the metas dominant in the included regions during the collection period.
- **Patch Bias:** The data reflects gameplay on specific game versions (see `matches` table `gameVersion` field). Major patches can significantly alter champion balance, items, and objectives, potentially making findings less relevant to different patches.
- **Missing Context:** The data captures *recorded* events and states but lacks external context like player communication (voice/text chat), player fatigue/tilt, real-time strategic intent, or external distractions.
- **API Limitations:** Data is subject to the accuracy and granularity provided by the Riot Games API. Some nuanced actions or states might not be perfectly captured. Rate limits inherent to the API restrict the size and frequency of potential dataset updates.

#### Recommendations

- Users should explicitly acknowledge the **high-elo (Challenger) bias** when reporting results and be cautious about generalizing findings to other player segments.
- Always consider the **game version (`gameVersion`)** when analyzing the data, as metas and balance change significantly between patches.
- Users **must** adhere to the **Riot Games API Terms of Service and Developer Policies** in all uses of this data.

## Uses

### Disclaimer

*This dataset utilizes data from the Riot Games API. Its use is subject to the Riot Games API Terms of Service and relevant developer policies. GPTilt is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.*

### License

This dataset and all associated code is licensed under the [Creative Commons Attribution-NonCommercial 4.0 International](https://creativecommons.org/licenses/by-nc/4.0/legalcode.en) license.

### Direct Use

This dataset is intended for **non-commercial research, data analysis, and exploration** aimed at understanding League of Legends gameplay dynamics, strategic patterns, champion interactions, and game flow. Suitable uses include:

- **Statistical analysis** of high-elo match characteristics.
- **Exploratory data analysis** to uncover **trends** and correlations.
- Training **machine learning models** (including Transformer-based architectures like LLoLMs) for tasks related to **game state representation**, event sequence modeling, pattern recognition for game understanding, etc.
- **Feature engineering** for derived metrics.
- **Educational purposes** related to data science and game analytics.

**Users must ensure their use case complies with the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) and [Developer Policies](https://developer.riotgames.com/policies/general). Consult these policies before using the data.**

### Out-of-Scope Use

This dataset **must not** be used for purposes that violate the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) or [Developer Policies](https://developer.riotgames.com/policies/general).

This dataset is derived from high-elo games and may not accurately represent gameplay patterns at lower skill levels. **Consult the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) and [Developer Policies](https://developer.riotgames.com/policies/general) for comprehensive usage restrictions.**

{% include changelist %}

## Citation

**If you wish to use this dataset in your work, we kindly ask that you cite it.**

For most informal work, a simple mention of the GPTilt project and the {{ pretty_name }} dataset will suffice.

**BibTeX:**

```bibtex
@misc{gptilt_{{ pretty_name | replace(" ", "_") | lower }},
  author    = { {{ curators | default("GPTilt Contributors", true) }} },
  title     = { {{ pretty_name }} },
  year      = { {{ creation_date  }} },
  publisher = { Hugging Face },
  journal   = { Hugging Face Hub },
  url       = { https://huggingface.co/datasets/gptilt/{{ id }} }
}
```
