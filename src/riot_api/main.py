import argparse
import asyncio
from riot_api import get, workers
import os
import storage
import threading


REGIONS_AND_PLATFORMS = {
    "americas": {
        "br1",
        "la1",
        "la2",
        "na1",
    },
    "asia": {
        "jp1",
        "kr",
        "vn2",
    },
    "europe": {
        "eun1",
        "euw1",
        "tr1",
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description="Run data pipeline in RAW or STG mode.")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    
    # Shared arguments
    def add_common_args(subparser):
        subparser.add_argument("--key", required=True, help="Riot API key")
        subparser.add_argument("--root", required=True, help="Root directory for storage")

    # RAW subcommand
    raw_parser = subparsers.add_parser("raw", help="Run the RAW data pipeline")
    add_common_args(raw_parser)

    # STG subcommand
    stg_parser = subparsers.add_parser("stg", help="Run the STG data pipeline")
    add_common_args(stg_parser)
    stg_parser.add_argument("--flush", action="store_true", help="Flush staging tables before running")

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Mode: {args.mode}")
    print(f"Root directory: {args.root}")
    os.environ["RIOT_API_KEY"] = args.key
    
    storage_raw = storage.Storage(
        args.root,
        'riot-api',
        'raw',
        ['player_match_ids', 'match_info', 'match_timeline']
    )
    storage_stg = storage.StorageParquet(
        args.root,
        'riot-api',
        'stg',
        tables=['matches', 'participants', 'events']
    )

    list_of_platforms = [
        (region, platform)
        for region, platforms in REGIONS_AND_PLATFORMS.items()
        for platform in platforms
    ]
    regions = REGIONS_AND_PLATFORMS.keys()

    # Retrieve player uuids
    print(f"Fetching player uuids from {'Riot API' if args.mode == 'raw' else 'raw'}...")
    dict_of_player_uuids = {
        region: [
            entry['puuid']
            for entry in asyncio.run(
                get.fetch_with_rate_limit('players', platform=platform)
            )["entries"]
        ] for region, platform in list_of_platforms
    } if args.mode == "raw" else {
        region: storage_raw.find_files('player_match_ids', f'region={region}/*')
        for region in regions
    }
    print(f"Found {len(dict_of_player_uuids)} player uuids.")

    list_of_threads = []

    for region in regions:
        print(f"[{region}] Starting thread...")
        list_of_threads.append(threading.Thread(
            target=(
                workers.stg if args.mode == "stg"
                else lambda *args, **kwargs: asyncio.run(workers.raw(*args, **kwargs))
            ),
            args=(region, dict_of_player_uuids[region], storage_raw),
            kwargs={
                "storage_stg": storage_stg,
                "flush": args.flush
            } if args.mode == "stg" else None
        ))
        
        list_of_threads[-1].start()

    for t in list_of_threads:
        t.join()
