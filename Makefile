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

dev: private
	. .venv/bin/activate && dagster dev