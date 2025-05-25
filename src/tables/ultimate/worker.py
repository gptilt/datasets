import common
import importlib
import storage


TABLES_PER_DATASET = {
    "events": ["events"],
    "snapshot": ["snapshot"]
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
    storage_ultimate = storage.StoragePartition(
        root=root,
        schema="ultimate",
        dataset=dataset,
        tables=TABLES_PER_DATASET[dataset],
        partition_col="region",
        partition_val=region
    )

    list_of_match_ids = storage_basic.load_to_polars("matches", ["matchId"])[:count]
    
    # Import the appropriate work function based on the dataset
    module_path = f".{dataset.replace('-', '_')}.main"

    # Dynamically import the module and get the worker
    try:
        work_module = importlib.import_module(module_path, package=__package__)
    except ModuleNotFoundError as e:
        raise ImportError(f"Could not import module {module_path}: {e}") from e
    
    for _ in common.work_generator(
        list_of_match_ids,
        work_module.main,
        descriptor=region,
        step=count // 100,  # Save shard every N matches
        # kwargs
        storage_basic=storage_basic,
        storage_ultimate=storage_ultimate
    ):
        if flush:
            storage_ultimate.flush()
