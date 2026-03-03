init:
	wget -qO- https://astral.sh/uv/install.sh | sh
	uv venv --clear && uv sync