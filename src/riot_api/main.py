import argparse
import asyncio
import os
from constants import REGIONS_AND_PLATFORMS
from common import print, multithreaded
from riot_api import worker


def parse_args():
    parser = argparse.ArgumentParser(description="Collect data from the Riot API.")
    
    parser.add_argument("--root", required=True, help="Root directory for storage")
    parser.add_argument("--key", required=True, help="Riot API key")

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Root directory: {args.root}")

    os.environ["RIOT_API_KEY"] = args.key

    multithreaded(
        iterable=REGIONS_AND_PLATFORMS.keys(),
        target=lambda *args, **kwargs: asyncio.run(worker.raw(*args, **kwargs)),
        args=(REGIONS_AND_PLATFORMS, args.root),
    )
