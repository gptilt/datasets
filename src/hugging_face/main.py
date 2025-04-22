import argparse
from huggingface_hub import DatasetCard, DatasetCardData
from huggingface_hub import HfApi
import os
from riot_api import disclaimer
import time


def parse_args():
    parser = argparse.ArgumentParser(description="Manage Hugging Face datasets.")
    
    parser.add_argument(
        "--root",
        required=True,
        help="Dataset directory. If uploading a specific schema (e.g. 'stg'), include the complete path."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    api = HfApi(token=os.getenv("HF_TOKEN"))
    api.upload_folder(
        folder_path=args.root,
        repo_id="gptilt/riot-match-challenger-10k",
        repo_type="dataset",
    )

    card_data = DatasetCardData(
        pretty_name='10K League of Legends Challenger Matches',
        dataset_summary="10,000 ranked League of Legends matches from the Challenger tier in 10 different regions.",
        multilinguality='monolingual',
    )
    card = DatasetCard.from_template(
        card_data,
        disclaimer=disclaimer.TEXT,
        creation_date=time.strftime("%Y"),
        id="riot-match-challenger-10k",
        template_path='./src/hugging_face/templates/datasets/match.md',
    )
    card.push_to_hub(
        repo_id="gptilt/riot-match-challenger-10k",
        repo_type="dataset",
        commit_message="Add dataset card",
    )
