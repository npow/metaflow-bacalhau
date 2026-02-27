"""
@bacalhau decorator — runs a Metaflow step on the Bacalhau distributed
compute network inside a Docker container.

Usage
-----
    from metaflow import FlowSpec, step
    from metaflow_extensions.bacalhau.plugins.bacalhau_decorator import bacalhau

    class MyFlow(FlowSpec):
        @bacalhau(cpu=2, memory=4096, image="python:3.11")
        @step
        def train(self):
            import torch
            ...
"""

import os
import platform
import sys

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    FEAT_ALWAYS_UPLOAD_CODE_PACKAGE,
)
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task

from .bacalhau_exceptions import BacalhauException


# ── Configuration keys (all readable from environment / metaflow config) ──────

# Default Docker image.  Falls back to matching the local Python version.
_BACALHAU_CONTAINER_IMAGE = os.environ.get("METAFLOW_BACALHAU_CONTAINER_IMAGE", "")
_BACALHAU_CONTAINER_REGISTRY = os.environ.get(
    "METAFLOW_BACALHAU_CONTAINER_REGISTRY", ""
)


class BacalhauDecorator(StepDecorator):
    """
    Specifies that this step should execute on the
    `Bacalhau <https://bacalhau.org>`_ distributed compute network.

    Parameters
    ----------
    cpu : int or str, default 1
        Number of CPUs required (Kubernetes-style, e.g. "2" or "500m").
        If ``@resources`` is also present, the larger value wins.
    gpu : int, default 0
        Number of GPUs required.
        If ``@resources`` is also present, the larger value wins.
    memory : int, default 4096
        Memory in MB.
        If ``@resources`` is also present, the larger value wins.
    image : str, optional
        Docker image to use.  Defaults to ``METAFLOW_BACALHAU_CONTAINER_IMAGE``
        or ``python:<major>.<minor>`` matching the local interpreter.
    timeout : int, optional
        Maximum wall-clock seconds to wait for the job (default: derived from
        ``@timeout`` decorator, or 5 days).
    labels : dict, optional
        Arbitrary string key/value labels attached to the Bacalhau job.
    """

    name = "bacalhau"

    defaults = {
        "cpu": None,
        "gpu": None,
        "memory": None,
        "image": None,
        "timeout": None,
        "labels": None,
    }

    resource_defaults = {
        "cpu": "1",
        "gpu": "0",
        "memory": "4096",
    }

    # Class-level cache: the code package is uploaded once and shared across
    # all tasks in the same run (mirrors the Batch / Kubernetes pattern).
    package_url = None
    package_sha = None
    package_metadata = None

    # Conda environment flag (mirrored from Batch/K8s)
    supports_conda_environment = True
    target_platform = "linux-64"

    # ------------------------------------------------------------------
    # Lifecycle hooks (called by Metaflow runtime in order)
    # ------------------------------------------------------------------

    def init(self):
        """Validate and fill in defaults for decorator attributes."""
        if not self.attributes["image"]:
            if _BACALHAU_CONTAINER_IMAGE:
                self.attributes["image"] = _BACALHAU_CONTAINER_IMAGE
            else:
                self.attributes["image"] = "python:{major}.{minor}".format(
                    major=platform.python_version_tuple()[0],
                    minor=platform.python_version_tuple()[1],
                )

        # Prepend registry if the image has no registry component.
        if _BACALHAU_CONTAINER_REGISTRY and "/" not in self.attributes["image"]:
            self.attributes["image"] = "{registry}/{image}".format(
                registry=_BACALHAU_CONTAINER_REGISTRY.rstrip("/"),
                image=self.attributes["image"],
            )

        if self.attributes["labels"] is None:
            self.attributes["labels"] = {}

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        """Called once per step after all decorators are initialised."""
        if flow_datastore.TYPE not in ("s3",):
            raise BacalhauException(
                "The @bacalhau decorator requires --datastore=s3.  "
                "Bacalhau containers need a cloud-accessible datastore to "
                "exchange code packages and task artifacts."
            )

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        # Merge @resources attributes (take the larger value per field).
        self.attributes.update(
            _compute_resource_attributes(decos, self, self.resource_defaults)
        )

        # Derive timeout from @timeout decorator if not set explicitly.
        if self.attributes["timeout"] is None:
            self.attributes["timeout"] = get_run_time_limit_for_task(decos)

    def runtime_init(self, flow, graph, package, run_id):
        """Called once before the runtime starts dispatching tasks."""
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        """Upload the code package to the datastore (once per run)."""
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        """Redirect step execution to the 'bacalhau step' CLI command."""
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["bacalhau", "step"]
            cli_args.command_args += [
                self.package_metadata,
                self.package_sha,
                self.package_url,
            ]
            cli_args.command_options.update(
                {k: v for k, v in self.attributes.items()}
            )
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        """Register Bacalhau execution metadata (runs inside the container)."""
        self.metadata = metadata
        self.task_datastore = task_datastore

        meta = {}
        job_id = os.environ.get("BACALHAU_JOB_ID")
        if job_id:
            meta["bacalhau-job-id"] = job_id
        node_id = os.environ.get("BACALHAU_NODE_ID")
        if node_id:
            meta["bacalhau-node-id"] = node_id
        execution_id = os.environ.get("BACALHAU_EXECUTION_ID")
        if execution_id:
            meta["bacalhau-execution-id"] = execution_id

        if meta:
            metadata.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field=k,
                        value=v,
                        type=k,
                        tags=["attempt_id:%d" % retry_count],
                    )
                    for k, v in meta.items()
                ],
            )

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        """Sync local metadata back to the datastore when metadata=local."""
        if os.environ.get("BACALHAU_JOB_ID"):
            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                from metaflow.metadata_provider.util import (
                    sync_local_metadata_to_datastore,
                )

                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

    # ------------------------------------------------------------------
    # Class-level helpers
    # ------------------------------------------------------------------

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        """Upload the code tarball to the datastore at most once per run."""
        if cls.package_url is not None:
            return
        if not FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
            cls.package_metadata = package.package_metadata
        else:
            cls.package_url = package.package_url()
            cls.package_sha = package.package_sha()
            cls.package_metadata = package.package_metadata


# ── Utility ───────────────────────────────────────────────────────────────────


def _compute_resource_attributes(decos, deco, defaults):
    """Merge @resources with the decorator's own resource settings.

    Takes the maximum value for each resource field so that either decorator
    can specify the binding constraint.
    """
    resources = dict(defaults)

    # Find @resources decorator if present.
    for d in decos:
        if d.name == "resources":
            for k in ("cpu", "gpu", "memory"):
                if d.attributes.get(k) is not None:
                    try:
                        resources[k] = str(
                            max(int(resources[k]), int(d.attributes[k]))
                        )
                    except (TypeError, ValueError):
                        resources[k] = str(d.attributes[k])

    # The decorator's own attributes take precedence if explicitly set.
    for k in ("cpu", "gpu", "memory"):
        if deco.attributes.get(k) is not None:
            try:
                resources[k] = str(max(int(resources[k]), int(deco.attributes[k])))
            except (TypeError, ValueError):
                resources[k] = str(deco.attributes[k])

    return {k: resources[k] for k in ("cpu", "gpu", "memory")}
