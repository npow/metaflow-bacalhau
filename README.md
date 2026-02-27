# metaflow-bacalhau

[![CI](https://github.com/npow/metaflow-bacalhau/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-bacalhau/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-bacalhau)](https://pypi.org/project/metaflow-bacalhau/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.8-3.11](https://img.shields.io/badge/python-3.8--3.11-blue.svg)](https://www.python.org/downloads/)
[![Status: WIP](https://img.shields.io/badge/status-WIP-yellow.svg)]()

> **Work in progress.** This project is under active development and not yet ready for production use.

Run Metaflow steps on distributed compute without rewriting your workflow.

## The problem

Metaflow makes local ML pipelines easy, but scaling individual steps to remote compute — GPUs, large memory, distributed jobs — means rewriting code for a different platform. You want one workflow definition that runs locally for debugging and remotely for production, without changing a line of step code.

Existing Metaflow backends (Batch, Kubernetes) tie you to AWS or a Kubernetes cluster. If your compute lives on the [Bacalhau](https://bacalhau.org) network, there's no path.

## Quick start

```bash
pip install metaflow-bacalhau
```

```python
from metaflow import FlowSpec, step, bacalhau

class MyFlow(FlowSpec):

    @step
    def start(self):
        self.next(self.train)

    @bacalhau(cpu=4, memory=16384, image="python:3.11")
    @step
    def train(self):
        # Runs on Bacalhau — zero code changes required
        import torch
        self.model = train_model()
        self.next(self.end)

    @step
    def end(self):
        print("done:", self.model)
```

```bash
export BACALHAU_API_HOST=your-bacalhau-node
export METAFLOW_DATASTORE_SYSROOT_S3=s3://your-bucket/metaflow
python myflow.py run
```

## Install

```bash
pip install metaflow-bacalhau
```

Requirements:

| Dependency | Notes |
|---|---|
| Metaflow ≥ 2.9 | Core framework |
| Bacalhau node | Any accessible Bacalhau API endpoint (v1.7+) |
| S3-compatible datastore | Code packages and artifacts are exchanged via S3 |
| Docker image | Must contain the same Python version as the local interpreter |

## Usage

### Basic step on Bacalhau

```python
from metaflow import bacalhau

@bacalhau(cpu=2, memory=8192, image="python:3.11-slim")
@step
def process(self):
    # This step runs in a Docker container on the Bacalhau network.
    # All self.* artifacts are persisted to S3 as normal.
    self.result = heavy_computation()
    self.next(self.end)
```

### Using @resources alongside @bacalhau

Both decorators cooperate — the larger value per resource dimension wins:

```python
@bacalhau(image="pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime")
@resources(gpu=1, memory=32768)
@step
def train_gpu(self):
    import torch
    print("GPU available:", torch.cuda.is_available())
    self.next(self.end)
```

### Custom labels for job filtering

```python
@bacalhau(
    cpu=1,
    memory=4096,
    image="python:3.11",
    labels={"team": "ml-platform", "env": "prod"},
)
@step
def score(self):
    self.next(self.end)
```

## Configuration

All configuration via environment variables:

```bash
# Bacalhau endpoint (defaults to localhost:1234)
export BACALHAU_API_HOST=bacalhau.example.com
export BACALHAU_API_PORT=1234

# Default container image (falls back to python:<local-major>.<minor>)
export METAFLOW_BACALHAU_CONTAINER_IMAGE=python:3.11-slim
export METAFLOW_BACALHAU_CONTAINER_REGISTRY=registry.example.com  # optional prefix

# S3 datastore (required)
export METAFLOW_DATASTORE_SYSROOT_S3=s3://my-bucket/metaflow
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# S3-compatible endpoint (e.g. MinIO for local dev)
export METAFLOW_S3_ENDPOINT_URL=http://localhost:9000
# Override endpoint URL injected into Bacalhau containers
export METAFLOW_BACALHAU_CONTAINER_S3_ENDPOINT_URL=http://host.docker.internal:9000
```

### Decorator reference

```python
@bacalhau(
    cpu=1,          # CPUs — int or Kubernetes-style string ("500m")
    gpu=0,          # GPUs
    memory=4096,    # Memory in MB
    image=None,     # Docker image (default: python:<major>.<minor>)
    timeout=None,   # Wall-clock seconds (default: from @timeout or 5 days)
    labels=None,    # dict of Bacalhau job labels
)
```

## How it works

1. The local orchestrator packages your flow code into a tarball and uploads it to S3 (once per run).
2. For each `@bacalhau` step, Metaflow's runtime calls `bacalhau step` instead of running locally.
3. That CLI command submits a Bacalhau Docker job containing your container image, a bootstrap command that downloads the code package from S3 and runs the Metaflow step, and all `METAFLOW_*` env vars (including AWS credentials) needed to read/write the S3 datastore.
4. The local process polls Bacalhau for completion and surfaces any errors.
5. Logs are written to S3 by the container via `mflog` and appear in the Metaflow UI normally.

## Local dev setup

Spin up a local Bacalhau devstack and MinIO S3 for testing without cloud dependencies:

```bash
bash dev/start.sh      # starts MinIO + Bacalhau devstack
source dev/env.sh      # sets BACALHAU_API_HOST, AWS_*, METAFLOW_* vars
python examples/e2e_test.py run
bash dev/stop.sh       # tears everything down
```

## Development

```bash
git clone https://github.com/npow/metaflow-bacalhau
cd metaflow-bacalhau
pip install -e ".[dev]"
pytest
```

## License

[Apache 2.0](LICENSE)
