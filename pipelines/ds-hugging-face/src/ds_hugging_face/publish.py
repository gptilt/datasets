"""
Publish a dataset descriptor to HuggingFace as one repo with one config per table.

`build_publish_definitions(ds)` returns the (asset, job, schedule) trio
for a single `Dataset`;
everything dataset-specific — repo, tables, license, template, source catalog, cron —
comes from the descriptor, so reviving/adding a dataset is just another `Dataset` + one call here.

Scheduled batch (unpartitioned):
each run reads the *current* snapshot of the clean Iceberg tables,
stages them as Parquet, uploads, and refreshes the card.
`deps` on the clean assets are lineage-only — the schedule fires after the upstream refresh.
"""
import pathlib
import tempfile
import time

import dagster as dg
import huggingface_hub as hub

from .hub import HuggingFaceHub
from .render import render_card_template
from .schemata import ESPORTS_ENTITIES, ESPORTS_MATCHES, Dataset


def build_publish_definitions(ds: Dataset):
    @dg.asset(
        name=f"publish_hugging_face_{ds.slug}",
        group_name="hugging_face",
        deps=ds.deps,
        required_resource_keys={ds.catalog_resource_key, "hugging_face_hub"},
    )
    def _publish(context: dg.AssetExecutionContext):
        catalog = getattr(context.resources, ds.catalog_resource_key)
        hf: HuggingFaceHub = context.resources.hugging_face_hub

        api = hf.api()
        api.create_repo(ds.repo_id, repo_type="dataset", exist_ok=True)

        row_counts: dict[str, int] = {}
        with tempfile.TemporaryDirectory() as tmp:
            staging = pathlib.Path(tmp)

            for table in ds.tables:
                df = catalog.load_table_to_polars(table)
                table_dir = staging / table
                table_dir.mkdir(parents=True, exist_ok=True)
                df.write_parquet(table_dir / f"{table}.parquet")
                row_counts[table] = len(df)
                context.log.info(f"Staged {table}: {len(df)} rows")

            api.upload_folder(
                repo_id=ds.repo_id,
                folder_path=str(staging),
                repo_type="dataset",
                commit_message="Update data",
            )

        # One config per table; clean tables are unpartitioned -> single `train` split.
        card_data = hub.DatasetCardData(
            license=ds.license,
            pretty_name=ds.pretty_name,
            configs=[
                {
                    "config_name": table,
                    "data_files": [{"split": "train", "path": f"{table}/*.parquet"}],
                }
                for table in ds.tables
            ],
        )
        card = hub.DatasetCard.from_template(
            card_data,
            template_path=render_card_template(
                ds.slug,
                id=ds.dataset_id,
                pretty_name=ds.pretty_name,
                dataset_summary=ds.dataset_summary,
                purpose=ds.purpose,
                license=ds.license,
                creation_date=time.strftime("%Y"),
                num_tables=len(ds.tables),
                example_table=next(iter(ds.tables)),
            ),
        )
        card.push_to_hub(
            repo_id=ds.repo_id,
            repo_type="dataset",
            commit_message="Update dataset card",
            # Pass the token explicitly
            token=hf.token,
        )

        return dg.MaterializeResult(
            metadata={
                "repo": dg.MetadataValue.url(f"https://huggingface.co/datasets/{ds.repo_id}"),
                "row_counts": dg.MetadataValue.json(row_counts),
                "tables": dg.MetadataValue.json(list(ds.tables)),
            }
        )

    job = dg.define_asset_job(
        name=f"job_publish_hugging_face_{ds.slug}",
        selection=[_publish],
    )

    @dg.schedule(
        name=f"schedule_publish_hugging_face_{ds.slug}",
        job=job,
        cron_schedule=ds.cron,
        execution_timezone="UTC",
    )
    def _schedule(context: dg.ScheduleEvaluationContext):
        return dg.RunRequest(run_key=context.scheduled_execution_time.date().isoformat())

    return _publish, job, _schedule


asset_publish_esports, job_publish_esports, schedule_publish_esports = (
    build_publish_definitions(ESPORTS_ENTITIES)
)

asset_publish_esports_matches, job_publish_esports_matches, schedule_publish_esports_matches = (
    build_publish_definitions(ESPORTS_MATCHES)
)
