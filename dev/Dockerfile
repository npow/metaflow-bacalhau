# Dev image that mirrors what @bacalhau steps will run inside.
# Build: docker build -f dev/Dockerfile -t metaflow-bacalhau-dev:latest .
FROM python:3.10-slim

# Install metaflow and our extension from the local source tree.
WORKDIR /pkg
COPY pyproject.toml README.md ./
COPY metaflow_extensions/ metaflow_extensions/
RUN pip install --no-cache-dir metaflow && pip install --no-cache-dir .

# Smoke-test from a neutral directory (running from /pkg would put the source
# tree on sys.path, causing Metaflow to detect a duplicate extension).
# Pre-import metaflow first so its plugin system loads our extension through the
# proper path before we reference BacalhauDecorator directly.
WORKDIR /
RUN python -c "import metaflow; from metaflow_extensions.bacalhau.plugins.bacalhau_decorator import BacalhauDecorator; print('OK')"
