import argparse
import asyncio
import os
from riot_api import workers
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
        subparser.add_argument("--root", required=True, help="Root directory for storage")

    # RAW subcommand
    raw_parser = subparsers.add_parser("raw", help="Run the RAW data pipeline")
    raw_parser.add_argument("--key", required=True, help="Riot API key")
    add_common_args(raw_parser)

    # STG subcommand
    stg_parser = subparsers.add_parser("stg", help="Run the STG data pipeline")
    add_common_args(stg_parser)
    stg_parser.add_argument("--flush", action="store_true", help="Flush staging tables before running")
    stg_parser.add_argument("--count", help="Number of games to process", type=int, default=10000)

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Mode: {args.mode}")
    print(f"Root directory: {args.root}")

    list_of_threads = []

    if args.mode == "raw":
        os.environ["RIOT_API_KEY"] = args.key

        for region in REGIONS_AND_PLATFORMS.keys():
            print(f"[{region}] Starting thread...")
            list_of_threads.append(threading.Thread(
                target=(lambda *args, **kwargs: asyncio.run(workers.raw(*args, **kwargs))),
                args=(region, REGIONS_AND_PLATFORMS[region], args.root),
                kwargs={}
            ))
    elif args.mode == "stg":
        for region in REGIONS_AND_PLATFORMS.keys():
            print(f"[{region}] Starting thread...")
            list_of_threads.append(threading.Thread(
                target=(workers.stg),
                args=(region, args.root),
                kwargs={
                    "count": args.count // len(REGIONS_AND_PLATFORMS.keys()),
                    "flush": args.flush,
                }
            ))
    
    for t in list_of_threads:
        t.start()

    for t in list_of_threads:
        t.join()
