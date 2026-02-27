#!/usr/bin/env bash
# Spin up the full local dev stack: MinIO + Bacalhau devstack.
# Usage: bash dev/start.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Starting MinIO..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --wait minio minio-init
echo "    MinIO API:     http://localhost:9000"
echo "    MinIO console: http://localhost:9001  (minioadmin / minioadmin)"

echo ""
echo "==> Building dev container image..."
docker build -f "$SCRIPT_DIR/Dockerfile" -t metaflow-bacalhau-dev:latest "$REPO_ROOT"

echo ""
echo "==> Starting Bacalhau devstack..."
BACALHAU_BIN="${HOME}/.local/bin/bacalhau"
if [[ ! -x "$BACALHAU_BIN" ]]; then
    echo "ERROR: bacalhau not found at $BACALHAU_BIN"
    echo "       Run: curl -sL https://get.bacalhau.org/install.sh | bash"
    exit 1
fi

# Kill any existing devstack.
pkill -f "bacalhau devstack" 2>/dev/null || true
sleep 1

LOG_FILE="/tmp/bacalhau-devstack.log"
echo "    Logs: $LOG_FILE"

BACALHAU_ENVIRONMENT=production \
  "$BACALHAU_BIN" devstack \
    --orchestrators 1 \
    --compute-nodes 1 \
    2>&1 | tee "$LOG_FILE" &

# Wait for the API to be ready.
echo -n "    Waiting for Bacalhau API"
for i in $(seq 1 30); do
    if "$BACALHAU_BIN" node list --api-host 127.0.0.1 --api-port 1234 &>/dev/null; then
        echo " ready."
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "==> Dev stack is up. Source the env file, then run an example:"
echo ""
echo "    source dev/env.sh"
echo "    python examples/hello_bacalhau.py run"
