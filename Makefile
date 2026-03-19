init:
	wget -qO- https://astral.sh/uv/install.sh | sh
	uv venv --clear && uv sync --group dev

submodules:
	git submodule update --init --recursive

private: submodules
	wget -qO- https://astral.sh/uv/install.sh | sh
	uv venv --clear && uv sync --extra private --group dev