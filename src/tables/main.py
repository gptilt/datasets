import argparse
from common import print, multithreaded
from constants import REGIONS_AND_PLATFORMS


def parse_args():
    parser = argparse.ArgumentParser(description="Run table transformation pipelines.")
    subparsers = parser.add_subparsers(dest="schema", required=True)

    schemata = {
        "base": ["riot-api"],
        "gold": ["events"],
    }

    def add_common_args(subparser):
        subparser.add_argument("--root", required=True, help="Root directory for storage.")
        subparser.add_argument("--flush", action="store_true", help="Flush buffered tables after running.")
        subparser.add_argument("--count", help="Number of games to process.", type=int, default=10000)

    for schema, tables in schemata.items():
        schema_parser = subparsers.add_parser(schema, help=f"Run data pipelines for schema: {schema.upper()}.")
        table_subparsers = schema_parser.add_subparsers(dest="table", required=True)

        for table_name in tables:
            table_parser = table_subparsers.add_parser(table_name, help=f"Table: {schema}.{table_name}.")
            add_common_args(table_parser)

    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Schema: {args.schema}")
    print(f"Root directory: {args.root}")

    # Import the appropriate worker based on the schema
    if args.schema == "base":
        if args.table == "riot-api":
            from .base.riot_api import worker
            kwargs = {}
    elif args.schema == "gold":
        kwargs = {}

    multithreaded(
        iterable=REGIONS_AND_PLATFORMS.keys(),
        target=worker.main,
        args=(
            args.root,
            args.count // len(REGIONS_AND_PLATFORMS.keys()),
            args.flush
        ),
    )
