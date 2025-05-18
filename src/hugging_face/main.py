import argparse
from huggingface_hub import DatasetCard, DatasetCardData
from huggingface_hub import HfApi
import os
from pathlib import Path
from riot_api import disclaimer
import time


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
        "--match-count",
        required=True,
        help="Number of matches in the dataset."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    schemas_and_datasets = {
        "basic": {
            "matches": {
                "pretty_name": '10K League of Legends Challenger Matches',
                "dataset_summary": "10,000 ranked League of Legends matches from the Challenger tier in 10 different regions.",
            }
        },
        "ultimate": {
            "events": {
                "pretty_name": '1M Enriched Events from 10K LoL Challenger Matches',
                "dataset_summary": "1M Enriched Events from 10,000 ranked LoL matches from the Challenger tier in 10 different regions.",
            }
        }
    }
    dataset_id = f"{args.schema}-{args.dataset}-challenger-{args.match_count}"
    repo_id = f"gptilt/{dataset_id}"

    api = HfApi(token=os.getenv("HF_TOKEN"))

    api.upload_folder(
        folder_path=Path(args.root, args.schema, args.dataset),
        repo_id=repo_id,
        repo_type="dataset",
    )

    card_data = DatasetCardData(
        pretty_name=schemas_and_datasets[args.schema][args.dataset]["pretty_name"],
        dataset_summary=schemas_and_datasets[args.schema][args.dataset]["dataset_summary"],
        multilinguality='monolingual',
    )
    card = DatasetCard.from_template(
        card_data,
        disclaimer=disclaimer.TEXT,
        creation_date=time.strftime("%Y"),
        id=dataset_id,
        template_path=f"./src/hugging_face/templates/{args.schema}/{args.dataset}.md",
    )
    card.push_to_hub(
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Add dataset card",
    )
