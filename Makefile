init:
	wget -qO- https://astral.sh/uv/install.sh | sh
	uv venv --clear && uv sync --group dev

private:
	wget -qO- https://astral.sh/uv/install.sh | sh
	uv venv --clear && uv sync --extra private --group dev