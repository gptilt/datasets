install:
	wget -qO- https://astral.sh/uv/install.sh | sh

# Without submodules
init: install
	uv venv --clear && uv sync --group dev

# With submodules
submodules:
	git submodule update --init --recursive

private: install submodules
	uv venv --clear && uv sync --extra private --group dev

# Local Dagster UI. Forces a project-local DAGSTER_HOME so `dagster dev` never
# loads the production dagster.yaml (Postgres, fed by DAGSTER_PG_* on the box) —
# an empty home directory means SQLite/local defaults. Run `make init`/`private`
# first to set up the venv.
dev: private
	@mkdir -p $(CURDIR)/.dagster_home
	. .venv/bin/activate && DAGSTER_HOME=$(CURDIR)/.dagster_home dagster dev