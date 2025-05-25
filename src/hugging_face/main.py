import argparse
from .card import create_dataset_card_template
import huggingface_hub as hub
import pathlib


def parse_args():
    parser = argparse.ArgumentParser(description="Manage Hugging Face datasets.")
    
    parser.add_argument(
        "--root",
        required=True,
        help="Dataset root directory."
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Dataset schema."
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset to upload."
    )
    parser.add_argument(
        "--variant",
        required=True,
        help="Dataset variant. Can be the number of samples."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    schemas_and_datasets = {
        "basic": {
            "matches": {
                "pretty_name": f'{args.variant.upper()} League of Legends Challenger Matches',
                "dataset_summary": "This dataset contains all data available in the Riot API for 10,000 ranked League of Legends matches from the Challenger tier in 10 different regions.",
                "tables": ["matches", "participants", "events"],
                "splits": ["region_americas", "region_asia", "region_europe"],
                "purpose": "It's a clean version of the API's data, improved for clarity and usability.",
            }
        },
        "ultimate": {
            "events": {
                "pretty_name": f'{args.variant.upper()} Enriched Events from 10K LoL Challenger Matches',
                "dataset_summary": f"{args.variant.upper()} Enriched Events from 10,000 ranked LoL matches from the Challenger tier in 10 different regions.",
                "tables": ["events"],
                "splits": ["region_americas", "region_asia", "region_europe"],
                "purpose": "It provides a comprehensive profile of each event, complete with pre-game and game state information.",
            },
            "snapshot": {
                "pretty_name": f"League of Legends Challenger Matches' Snapshots At 15 Minutes",
                "dataset_summary": f"Snapshots of League of Legends Matches taken at 15 minutes, taken from 10,000 ranked LoL matches from the Challenger tier in 10 different regions.",
                "tables": ["snapshot"],
                "splits": ["region_americas", "region_asia", "region_europe"],
                "purpose": "Provides a complete snapshot of the game at 15 minutes.",
            }
        }
    }

    dataset_id = f"lol-{args.schema}-{args.dataset}-challenger-{args.variant}"
    dataset_details = schemas_and_datasets[args.schema][args.dataset]
    
    repo_id = f"gptilt/{dataset_id}"

    api = hub.HfApi()
    api.upload_folder(
        repo_id=repo_id,
        folder_path=pathlib.Path(args.root, args.schema, args.dataset),
        repo_type="dataset",
    )

    card_data = hub.DatasetCardData(
        configs=[{
            "config_name": table,
            "data_files": [{
                "split": split,
                "path": f"{table}/{split}*.parquet"
            } for split in dataset_details["splits"] ]
        } for table in dataset_details["tables"] ]
    )
    card = hub.DatasetCard.from_template(
        card_data,
        template_path=create_dataset_card_template(
            args.schema,
            args.dataset,
            dataset_id=dataset_id,
            dataset_details=dataset_details
        ),
    )
    card.push_to_hub(
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Add dataset card",
    )
