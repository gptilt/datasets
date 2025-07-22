import common
import importlib
import numpy as np
import polars as pl
import sklearn
import storage



TABLES_PER_DATASET = {
    "events": ["events"],
    "snapshot": ["snapshot"]
}


def train_test_split(
    groups: np.ndarray,
    stratify_labels: np.ndarray
):
    # Compute unique counts of stratified labels
    # to ensure there is at least one sample of that stratifying label for every split.
    _, counts = np.unique(stratify_labels, return_counts=True)

    # Stratified Group K-Fold split
    sgkf = sklearn.model_selection.StratifiedGroupKFold(
        n_splits=min(counts),
        shuffle=True,
        random_state=42
    )
    train_idx, test_idx = next(sgkf.split(
        np.zeros(len(groups)),
        stratify_labels,
        groups
    ))

    # Get train/test match IDs
    return {
        'train': groups[train_idx],
        'test': groups[test_idx]
    }


def main(
    region: str,
    root: str,
    dataset: str,
    count: int = 100,
    flush: bool = True,
    overwrite: bool = False,
):
    storage_basic = storage.StoragePartition(
        root=root,
        schema="basic",
        dataset="matches",
        tables=["matches", "participants", "events"],
        partition_col="region",
        partition_val=region
    )

    df_match_ids_with_game_version = storage_basic.load_to_polars(
        "matches",
        ["matchId", "gameVersion", "winningTeam"]
    )[:count].with_columns(
        # Concatenate stratification columns to produce a single stratify label
        pl.concat_str(["gameVersion", "winningTeam"], separator="_").alias("gameVersion_winningTeam")
    )

    dict_of_match_id_splits = train_test_split(
        groups=df_match_ids_with_game_version["matchId"].to_numpy(),
        stratify_labels=df_match_ids_with_game_version["gameVersion_winningTeam"].to_numpy()
    )

    # Import the appropriate work function based on the dataset
    module_path = f".{dataset.replace('-', '_')}.main"

    # Dynamically import the module and get the worker
    try:
        work_module = importlib.import_module(module_path, package=__package__)
    except ModuleNotFoundError as e:
        raise ImportError(f"Could not import module {module_path}: {e}") from e
    

    for split in ['train', 'test']:
        storage_ultimate = storage.StoragePartition(
            root=root,
            schema="ultimate",
            dataset=dataset,
            tables=TABLES_PER_DATASET[dataset],
            partition_col="region",
            partition_val=region,
            split=split
        )

        for _ in common.work_generator(
            dict_of_match_id_splits[split],
            work_module.main,
            descriptor=region,
            step=count // 100,  # Save shard every N matches
            # kwargs
            storage_basic=storage_basic,
            storage_ultimate=storage_ultimate
        ):
            if flush:
                storage_ultimate.flush()
