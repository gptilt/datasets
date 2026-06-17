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
# an empty home directory means SQLite/local defaults.
dev: private
	@mkdir -p $(CURDIR)/.dagster_home
	. .venv/bin/activate && DAGSTER_HOME=$(CURDIR)/.dagster_home \
		dagster dev \
			-m orchestration.chatbot \
			-m orchestration.leaguepedia \
			-m orchestration.riot_api

# Validate every code location locally.
# One `validate` per module so each loads in-process;
# passing all `-m` at once would make it a multi-location workspace that starts a
# gRPC server per location and may hang. `&&` stops at the first failure.
validate: private
	@mkdir -p $(CURDIR)/.dagster_home
	. .venv/bin/activate && export DAGSTER_HOME=$(CURDIR)/.dagster_home && \
		dagster definitions validate -m orchestration.chatbot && \
		dagster definitions validate -m orchestration.leaguepedia && \
		dagster definitions validate -m orchestration.riot_api