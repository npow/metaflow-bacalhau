#!/usr/bin/env bash
# Source this to configure the shell for local MinIO + Bacalhau dev.
#
#   source dev/env.sh

# ── MinIO (S3-compatible) ─────────────────────────────────────────────────────
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

# ── Metaflow → MinIO ─────────────────────────────────────────────────────────
export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_DATASTORE_SYSROOT_S3=s3://metaflow
# Use localhost for the orchestrator process (host → Docker port mapping)
export METAFLOW_S3_ENDPOINT_URL=http://localhost:9000
export METAFLOW_S3_VERIFY_CERTIFICATE=0
# Bacalhau containers use host.docker.internal to reach the host-mapped MinIO port
export METAFLOW_BACALHAU_CONTAINER_S3_ENDPOINT_URL=http://host.docker.internal:9000

# ── Bacalhau ──────────────────────────────────────────────────────────────────
export BACALHAU_API_HOST=127.0.0.1
export BACALHAU_API_PORT=1234

# ── Metaflow container image (must have metaflow + our extension installed) ───
export METAFLOW_BACALHAU_CONTAINER_IMAGE=metaflow-bacalhau-dev:latest

echo "Dev environment loaded."
echo "  MinIO console: http://localhost:9001  (minioadmin / minioadmin)"
echo "  Bacalhau API:  http://\$BACALHAU_API_HOST:\$BACALHAU_API_PORT"
