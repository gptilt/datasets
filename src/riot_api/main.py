import argparse
import asyncio
from constants import REGIONS_AND_PLATFORMS
from common import print, multithreaded
import dagster as dg
import os
from riot_api import worker
import storage


def new_storage(schema: str, kind: str):
    match kind:
        case 's3':
            storage_class = storage.StorageS3
        case 'iceberg':
            storage_class = storage.StorageIceberg
        case _:
            raise NotImplementedError(f"Invalid storage class: {kind}")
        
    match schema:
        case 'raw':
            return storage_class(
                root='gptilt',
                schema='raw',
                dataset='riot_api',
                tables=['league_entries'],
                bucket_name=dg.EnvVar("S3_BUCKET_NAME").get_value(),
                bucket_url=dg.EnvVar("S3_BUCKET_URL").get_value(),
                bucket_access_key=dg.EnvVar("S3_BUCKET_ACCESS_KEY").get_value(),
                bucket_secret_key=dg.EnvVar("S3_BUCKET_SECRET_KEY").get_value(),
            )
        case _:
            raise NotImplementedError(f"Invalid schema: {schema}")


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
