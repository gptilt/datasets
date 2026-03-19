# Old Dataset Tiering

This document explains the previous dataset nomenclature.

> The datasets that were created at the time are still available in their respective links, so feel free to use them!

## Basic

Datasets are split into two tiers:

- `basic`: Basic datasets are true to the raw data, with some additional transformations that improve coherence (usage of `matchId` instead of `gameId`), increase usability (e.g. the addition of inventory data to the `events` table in the `matches` dataset), reduce the scope (fields that aren't particularly relevant are removed), and denormalize (e.g. splitting kill events into kill and assist events) the underlying data.
- `ultimate`: Ultimate datasets, on the other hand, perform opinionated transformations, with some specific goal in mind. Data accuracy remains, but the dataset structure is markedly different.

### Matches

The match dataset contains all data available in the Riot API for a given set of matches.
Currently, the dataset can be found in the following variants:

- [`10k` Challenger matches](https://huggingface.co/datasets/gptilt/lol-basic-matches-challenger-10k), includes over 15M events from ranked matches with at least one challenger player. The 10 largest regions are included.

## Ultimate

### Events

The `ultimate` tier `events` dataset contains enriched events from a selection of matches from the Riot Games API. This dataset was specifically designed for time series analysis, with all events being timestamped and providing improved (compared to the `basic` tier `events` dataset) contextual information about every event.

Currently, the dataset can be found in the following variants:

- [Over `10M` events from `10K` Challenger matches](https://huggingface.co/datasets/gptilt/lol-ultimate-events-challenger-10m), includes over 10M events from ranked matches with at least one challenger player. Events are enriched with pregame context (player champions), and up-to-date game state context (inventories, corrected levels). The 10 largest regions are included.

### Snapshot

The `ultimate` tier `snapshot` dataset contains snapshots from a selection of matches from the Riot Games API. This dataset was specifically designed for isolated estimation/classification, namely win probability estimation, with all the relevant game state information included.

Currently, the dataset can be found in the following variants:

- [Snapshots at 15 minutes events from `10K` Challenger matches](https://huggingface.co/datasets/gptilt/lol-ultimate-snapshot-challenger-15min). Events were enriched with pregame context (player champions), and up-to-date game state context (inventories, corrected levels), then a snapshot at 15 minutes was taken. The 10 largest regions are included.
