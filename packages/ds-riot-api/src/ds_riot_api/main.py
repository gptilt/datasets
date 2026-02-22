import argparse
import asyncio
from ds_common import print, multithreaded
import os
from .constants import REGIONS_AND_PLATFORMS, worker


def parse_args():
    parser = argparse.ArgumentParser(description="Collect data from the Riot API.")
    
    parser.add_argument("--root", required=True, help="Root directory for storage")
    parser.add_argument("--key", required=True, help="Riot API key")
    parser.add_argument("--workload", required=True, help="Ingestion workload to execute")

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Root directory: {args.root}")

    os.environ["RIOT_API_KEY"] = args.key

    match args.workload:
        case 'match':
            worker_function = worker.match
        case 'mastery':
            worker_function = worker.mastery
        case _:
            raise ValueError("Specified workload does not exist!")

    multithreaded(
        iterable=REGIONS_AND_PLATFORMS.keys(),
        target=lambda *args, **kwargs: asyncio.run(worker_function(*args, **kwargs)),
        args=(REGIONS_AND_PLATFORMS, args.root),
    )
