#!/usr/bin/env bash
# Tear down the local dev stack.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Stopping Bacalhau devstack..."
pkill -f "bacalhau devstack" 2>/dev/null && echo "    stopped." || echo "    (not running)"

echo "==> Stopping MinIO..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
