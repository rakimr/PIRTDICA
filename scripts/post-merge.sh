#!/bin/bash
set -e

if command -v uv >/dev/null 2>&1; then
    uv sync
elif [ -f pyproject.toml ]; then
    python -m pip install -e . --quiet
fi
