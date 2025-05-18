import argparse
from common import print, multithreaded
from constants import REGIONS_AND_PLATFORMS
import importlib


def parse_args():
    parser = argparse.ArgumentParser(description="Run table transformation pipelines.")
    subparsers = parser.add_subparsers(dest="schema", required=True)

    schemata = {
        "basic": ["matches"],
        "ultimate": ["events"],
    }

    def add_common_args(subparser):
        subparser.add_argument("--root", required=True, help="Root directory for storage.")
        subparser.add_argument("--flush", action="store_true", help="Flush buffered tables after running.")
        subparser.add_argument("--count", help="Number of games to process.", type=int, default=10000)
        subparser.add_argument("--overwrite", action="store_true", help="Overwrite existing records.")

    for schema, datasets in schemata.items():
        schema_parser = subparsers.add_parser(schema, help=f"Run data pipelines for schema: {schema.upper()}.")
        dataset_subparsers = schema_parser.add_subparsers(dest="dataset", required=True)

        for dataset in datasets:
            table_parser = dataset_subparsers.add_parser(dataset, help=f"Table: {schema}.{dataset}.")
            add_common_args(table_parser)

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Schema: {args.schema}")
    print(f"Root directory: {args.root}")

    # Import the appropriate worker based on the schema
    module_path = f".{args.schema}.{args.dataset.replace('-', '_')}.worker"  # Importlib doesn't allow hyphens

    # Dynamically import the module and get the worker
    try:
        worker_module = importlib.import_module(module_path, package=__package__)
    except ModuleNotFoundError as e:
        raise ImportError(f"Could not import module {module_path}: {e}") from e

    multithreaded(
        iterable=REGIONS_AND_PLATFORMS.keys(),
        target=worker_module.main,
        args=(
            args.root,
            args.count // len(REGIONS_AND_PLATFORMS.keys()),
            args.flush,
            args.overwrite,
        ),
    )
