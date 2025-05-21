from jinja2 import Environment, FileSystemLoader
import pathlib
from riot_api import disclaimer
import time


def create_dataset_card_template(
    schema: str,
    dataset: str,
    dataset_id: str,
    dataset_details: dict
) -> str:
    env = Environment(loader=FileSystemLoader('./src/hugging_face/templates'))

    directory_relative_path = pathlib.Path(schema, dataset)
    rendered = env.get_template('main.md').render(
        changelist = str(directory_relative_path / "changelist.md"),
        tables = str(directory_relative_path / "tables.md"),
        transformations = str(directory_relative_path / "transformations.md"),

        card_data="{{ card_data }}",
        pretty_name=dataset_details["pretty_name"],
        id=dataset_id,
        dataset_summary=dataset_details["dataset_summary"],
        disclaimer=disclaimer.TEXT,
        creation_date=time.strftime("%Y"),
        multilinguality='monolingual',
        primary_language='english',
        example_table=dataset_details["tables"][0],
        example_split=dataset_details["splits"][0],
        purpose=dataset_details["purpose"],
        num_splits=len(dataset_details["splits"]),
        splits=dataset_details["splits"]
    )

    template_path = "output/dataset_card.md"
    with open(template_path, "w") as f:
        f.write(rendered)

    return template_path
