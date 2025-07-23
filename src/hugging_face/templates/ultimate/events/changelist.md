## Changelist

### Jul 23

- Added event type embeddings, i.e., language model embeddings computed using the mini sentence transformer model `all-MiniLM-L6-v2` and reduced via PCA.

### Jul 15

- Replaced `gameVersion` (e.g. `15.4.668.4304`) by `patch` (`15.4`).

### May 27, 2025

- Divided splits into `train` and `test`.

### May 26, 2025

- Per-player inventories (`inventory_`) were split into padded (thus, equal-length) inventory item IDs (`inventoryIds_`) and inventory item counts (`inventoryCounts_`).

### May 24, 2025

- Added fields `gameVersion`, `platformId` (server), and `gameStartTimestamp`.