"""
Render a dataset card from the Jinja templates under `templates/<template_dir>/`.

Generic over the dataset: the caller passes the template subtree and every card variable.
The literal `{{ card_data }}` placeholder is left intact
for HuggingFace's own second Jinja pass (`DatasetCard.from_template`),
which fills it with the YAML metadata block.
"""
import pathlib
import tempfile
from importlib.resources import files

from jinja2 import Environment, FileSystemLoader


def render_card_template(template_dir: str, **variables) -> str:
    """
    Pre-render `templates/<template_dir>/card.md` to a temp file; return its path.

    The `tables` / `transformations` / `changelist` partials are pulled in via
    `{% include %}` from the same subtree, so they're wired up automatically.
    """
    templates_root = pathlib.Path(str(files("ds_hugging_face"))) / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_root)))

    rendered = env.get_template(f"{template_dir}/card.md").render(
        card_data="{{ card_data }}",
        tables=f"{template_dir}/tables.md",
        transformations=f"{template_dir}/transformations.md",
        changelist=f"{template_dir}/changelist.md",
        **variables,
    )

    out_path = pathlib.Path(tempfile.mkdtemp(prefix="hf_card_")) / "README.md"
    out_path.write_text(rendered)
    return str(out_path)
