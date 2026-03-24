#!/bin/bash

# Ensure the script stops if any command fails
set -e

echo "====================================="
echo "  DQE - Test Suite Runner"
echo "====================================="

# Ensure project dependencies are strictly synced including our test extras
echo "1. Syncing development dependencies..."
uv sync --all-extras

# Set the Python path so the `src/dqe` package is recognizable without needing to pip install it
export PYTHONPATH=src

# Run Pytest explicitly pointed at the `tests/` directory to avoid rogue testing files in the root folder
echo "2. Running unit and integration tests..."
uv run pytest tests/ "$@"

echo "====================================="
echo "  Tests completed successfully!"
echo "====================================="
