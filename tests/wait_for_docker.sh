#!/bin/bash
# Wait for all Docker Compose services to be healthy before running tests.
# Usage: bash tests/wait_for_docker.sh [timeout_seconds]

set -e

TIMEOUT=${1:-90}
COMPOSE_FILE="docker-compose.yml"

echo "Waiting for Docker services to be healthy (timeout: ${TIMEOUT}s)..."

SECONDS=0
while true; do
    if [ "$SECONDS" -ge "$TIMEOUT" ]; then
        echo "ERROR: Timed out after ${TIMEOUT}s waiting for services."
        docker compose -f "$COMPOSE_FILE" ps
        exit 1
    fi

    # Count unhealthy/starting services
    UNHEALTHY=$(docker compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null | \
        python3 -c "
import sys, json
lines = sys.stdin.read().strip().split('\n')
count = 0
for line in lines:
    if not line.strip():
        continue
    try:
        svc = json.loads(line)
        health = svc.get('Health', '')
        state = svc.get('State', '')
        if state == 'running' and health != 'healthy':
            count += 1
    except json.JSONDecodeError:
        pass
print(count)
" 2>/dev/null || echo "99")

    if [ "$UNHEALTHY" = "0" ]; then
        echo "All services are healthy! (took ${SECONDS}s)"
        docker compose -f "$COMPOSE_FILE" ps
        exit 0
    fi

    sleep 3
done
