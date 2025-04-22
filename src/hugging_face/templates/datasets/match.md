---
{{ card_data }}
---

# Dataset Card for {{ pretty_name | default("League of Legends Match Data by GPTilt", true) }}

This dataset is part of the [GPTilt](https://github.com/gptilt) open-source initiative, aimed at advancing the community's understanding of League of Legends through data science and AI. It provides detailed data from high-elo matches.

## Dataset Details

### Dataset Description

This dataset contains **{{ pretty_name }}**. It was collected and processed via the official Riot Games API as part of the GPTilt project. The primary goals are to democratize access to high-quality LoL data for research and analysis, and to foster public exploration into game dynamics using data-driven methods.

The data is structured into three tables:

* **matches**: Contains match-level metadata (e.g., `matchId`, `gameDuration`, `gameVersion`, `winningTeam`).
* **participants**: Contains details for each of the 10 participants per match (e.g., `puuid`, `championId`, `teamId`, final stats like kills, deaths, assists, gold earned, items).
* **events**: Contains a detailed timeline of in-game events (e.g., `CHAMPION_KILL`, `ITEM_PURCHASED`, `WARD_PLACED`, `BUILDING_KILL`, `ELITE_MONSTER_KILL`) with timestamps, positions (where applicable), involved participants/items, etc. It also includes periodic snapshots (typically per minute) of participant states (`participantFrames`).

**Disclaimer:** {{ disclaimer | default("**Disclaimer:** *This dataset utilizes data from the Riot Games API. Its use is subject to the Riot Games API Terms of Service and relevant developer policies. GPTilt is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.*", true) }}

* **Curated by:** {{ curators | default("GPTilt Project Contributors", true) }}
* **Language(s):** The primary data is numerical, categorical IDs, timestamps, and coordinates. Text fields within the raw API data (e.g., champion names if resolved) may originate from various game client languages, but English (`en`) is the most common reference.
* **License:** {{ license | default("cc-by-nc-4.0", true) }}

## Uses

### Direct Use

This dataset is intended for **non-commercial research, data analysis, and exploration** aimed at understanding League of Legends gameplay dynamics, strategic patterns, champion interactions, and game flow. Suitable uses include:

* Statistical analysis of high-elo match characteristics.
* Exploratory data analysis to uncover trends and correlations.
* Training machine learning models (including Transformer-based architectures like LLoLMs) for tasks related to **game state representation, event sequence modeling, and pattern recognition for game understanding.**
* Feature engineering for derived metrics.
* Educational purposes related to data science and game analytics.

**Users must ensure their use case complies with the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) and [Developer Policies](https://developer.riotgames.com/policies/general). Consult these policies before using the data.**

### Out-of-Scope Use

This dataset **must not** be used for purposes that violate the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) or [Developer Policies](https://developer.riotgames.com/policies/general).

This dataset is derived from high-elo games and may not accurately represent gameplay patterns at lower skill levels. **Consult the Riot Games API [Terms of Service](https://developer.riotgames.com/terms) and [Developer Policies](https://developer.riotgames.com/policies/general) for comprehensive usage restrictions.**

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
5. **Staging & Transformation:** Raw data was parsed, and transformed into three structured tables (`matches`, `participants`, `events`). Event data includes periodic (typically 1-minute) snapshots (`participantFrames`) alongside discrete events.
6. **Output:** Data was written to Parquet files, partitioned by `region`. Key libraries used include `pyarrow`.

#### Who are the source data producers?

The underlying gameplay data is generated by **League of Legends players** participating in high-elo ranked matches. The **Riot Games API** serves as the source interface providing access to this gameplay data. The dataset curators are the contributors to the GPTilt project who performed the collection and processing steps. No demographic information about the players is collected, besides the region.

#### Personal and Sensitive Information

The dataset contains **PUUIDs** and **Participant IDs**, which are pseudonymous identifiers linked to League of Legends accounts. No other Personally Identifiable Information (PII) like real names, emails, or addresses is included. Use of these identifiers is subject to Riot Games' policies. Users should exercise caution and adhere to these policies, avoiding attempts to [deanonymize players who cannot reasonably be identified from visible information](https://developer.riotgames.com/policies/general#_developer-safety).

## Bias, Risks, and Limitations

* **Skill Tier Bias:** This dataset focuses *exclusively* on the Challenger tier. Findings may not generalize to other skill levels (Bronze, Silver, Gold, Platinum, Diamond, Master, Grandmaster) where metas, champion picks, and strategic execution differ significantly. Because match data is selected by searching for Challenger players, multi-tier games may (and are expected) to be present in the dataset.
* **Regional Bias:** While collected from multiple regions, the distribution might not be perfectly balanced, potentially reflecting the metas dominant in the included regions during the collection period.
* **Patch Bias:** The data reflects gameplay on specific game versions (see `matches` table `gameVersion` field). Major patches can significantly alter champion balance, items, and objectives, potentially making findings less relevant to different patches.
* **Missing Context:** The data captures *recorded* events and states but lacks external context like player communication (voice/text chat), player fatigue/tilt, real-time strategic intent, or external distractions.
* **API Limitations:** Data is subject to the accuracy and granularity provided by the Riot Games API. Some nuanced actions or states might not be perfectly captured. Rate limits inherent to the API restrict the size and frequency of potential dataset updates.
* **Risk of Misuse:** The dataset could potentially be misused for applications violating Riot Games' ToS (see Out-of-Scope Use). Users are responsible for compliance and GPTilt takes no responsibility for any damages.

### Recommendations

* Users should explicitly acknowledge the **high-elo (Challenger) bias** when reporting results and be cautious about generalizing findings to other player segments.
* Always consider the **game version (`gameVersion`)** when analyzing the data, as metas and balance change significantly between patches.
* Users **must** adhere to the **Riot Games API Terms of Service and Developer Policies** in all uses of this data.
* Avoid using this dataset for prohibited purposes, including creating cheats, bots, or unauthorized real-time coaching tools.
* Exercise caution regarding player identifiers (PUUIDs) and avoid attempts at deanonymization.

## Citation

Please cite the dataset if you use it in your work:

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
