#!/bin/bash

# Ensure the script stops if any command fails
set -e

echo "====================================="
echo "  DQE - Test Suite Runner"
echo "====================================="

# Parse arguments for tier-based testing
TIER_ARGS=""
PYTEST_EXTRA_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --embedded)
            TIER_ARGS="--tier embedded"
            ;;
        --docker)
            TIER_ARGS="--tier embedded --tier docker"
            ;;
        --docker-complex)
            TIER_ARGS="--tier docker_complex"
            ;;
        --all)
            TIER_ARGS=""  # no filter = run all
            ;;
        *)
            PYTEST_EXTRA_ARGS+=("$arg")
            ;;
    esac
done

# Ensure project dependencies are strictly synced including our test extras
echo "1. Syncing development dependencies..."
uv sync --all-extras

# Set the Python path so the `src/dqe` package is recognizable without needing to pip install it
export PYTHONPATH=src

# Run Pytest explicitly pointed at the `tests/` directory to avoid rogue testing files in the root folder
echo "2. Running tests..."
if [ -n "$TIER_ARGS" ]; then
    echo "   Tiers: $TIER_ARGS"
fi

uv run pytest tests/ $TIER_ARGS "${PYTEST_EXTRA_ARGS[@]}"

echo "====================================="
echo "  Tests completed successfully!"
echo "====================================="
