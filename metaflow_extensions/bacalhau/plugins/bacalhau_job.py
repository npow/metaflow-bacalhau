"""
Bacalhau job submission and monitoring for Metaflow.

Handles building the container command, submitting jobs to the Bacalhau
network, polling for completion, and surfacing logs/errors back to the
local orchestrator.
"""

import os
import shlex
import sys
import time

# Lazy import: mflog and metaflow.exception are only imported when needed
# (inside methods) so this module can be loaded safely during Metaflow's
# own plugin-discovery phase without triggering a circular import.

# Bacalhau execution state integers (from pkg/models/execution_state_string.go)
_EXEC_COMPLETED = "9"
_EXEC_FAILED = "10"
_EXEC_CANCELLED = "11"
_TERMINAL_STATES = {_EXEC_COMPLETED, _EXEC_FAILED, _EXEC_CANCELLED}
_FAILED_STATES = {_EXEC_FAILED, _EXEC_CANCELLED}

# Paths inside the container where mflog writes structured logs
_LOGS_DIR = "$PWD/.logs"
_STDOUT_PATH = os.path.join(_LOGS_DIR, "mflog_stdout")
_STDERR_PATH = os.path.join(_LOGS_DIR, "mflog_stderr")

# How often (seconds) to poll Bacalhau for job status
_POLL_INTERVAL = 5

from .bacalhau_exceptions import (  # noqa: E402 — after module-level constants
    BacalhauException,
    BacalhauKilledException,
    _upgrade_bases,
)


class BacalhauJob:
    """Submit and monitor a single Metaflow step on the Bacalhau network."""

    def __init__(self, metadata, environment):
        self.metadata = metadata
        self.environment = environment
        self._job_id = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def launch_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        datastore_type,
        image,
        cpu=None,
        gpu=None,
        memory=None,
        timeout=5 * 24 * 60 * 60,
        env=None,
        labels=None,
    ):
        """Build and submit a Bacalhau job, then block until it completes.

        Parameters
        ----------
        step_name : str
        step_cli : str
            Full shell command to run the Metaflow step inside the container.
        task_spec : dict
            Keys: flow_name, step_name, run_id, task_id, retry_count.
        code_package_metadata, code_package_sha, code_package_url : str
            Identifiers for the uploaded code tarball.
        datastore_type : str
            e.g. "s3".
        image : str
            Docker image to use.
        cpu : str, optional
            Kubernetes-style CPU string, e.g. "2" or "500m".
        gpu : str, optional
            Number of GPUs as a string, e.g. "1".
        memory : str, optional
            Memory string, e.g. "4096" (MB) or "4Gi".
        timeout : int
            Seconds before giving up (default 5 days).
        env : dict, optional
            Extra environment variables to inject.
        labels : dict, optional
            Bacalhau job labels for filtering/identification.
        """
        # Upgrade exception bases to MetaflowException now that Metaflow is live.
        _upgrade_bases()

        from bacalhau_apiclient.models.api_put_job_request import ApiPutJobRequest
        from bacalhau_apiclient.models.job import Job
        from bacalhau_apiclient.models.resources_config import ResourcesConfig
        from bacalhau_apiclient.models.spec_config import SpecConfig
        from bacalhau_apiclient.models.task import Task
        from bacalhau_sdk.jobs import Jobs

        command = self._build_command(
            datastore_type,
            code_package_metadata,
            code_package_url,
            step_name,
            [step_cli],
            task_spec,
        )

        container_env = self._build_env(
            task_spec,
            code_package_metadata,
            code_package_sha,
            code_package_url,
            datastore_type,
            extra=env or {},
        )

        # Bacalhau engine params use PascalCase keys matching Go struct fields.
        # EnvironmentVariables is a list of "KEY=VALUE" strings.
        engine_params = {
            "Image": image,
            "Entrypoint": ["/bin/bash"],
            # command is ["bash", "-c", "<cmd_str>"] — drop the outer bash
            # since Entrypoint is already /bin/bash.
            "Parameters": command[1:],
            "EnvironmentVariables": [
                "%s=%s" % (k, v) for k, v in container_env.items()
            ],
        }

        resources = ResourcesConfig(
            cpu=str(cpu) if cpu else "1",
            memory=self._normalise_memory(memory),
            gpu=str(gpu) if gpu else "0",
        )

        task = Task(
            name="metaflow-step",
            engine=SpecConfig(type="docker", params=engine_params),
            resources=resources,
            publisher=SpecConfig(),
        )

        job_name = "mf-{flow}-{step}-{run}-{task}".format(
            flow=task_spec["flow_name"].lower(),
            step=task_spec["step_name"].lower(),
            run=task_spec["run_id"],
            task=task_spec["task_id"],
        )

        job = Job(
            name=job_name,
            type="batch",
            count=1,
            tasks=[task],
            labels=labels or {},
        )

        jobs_client = Jobs()
        response = jobs_client.put(ApiPutJobRequest(job=job))
        if response is None:
            raise BacalhauException(
                "Failed to submit Bacalhau job — check that the Bacalhau "
                "API is reachable (BACALHAU_API_HOST / BACALHAU_API_PORT)."
            )

        self._job_id = response.job_id
        return self._wait(jobs_client, self._job_id, timeout)

    def kill(self):
        if self._job_id is None:
            return
        try:
            from bacalhau_sdk.jobs import Jobs

            Jobs().stop(self._job_id, reason="Metaflow kill request")
        except Exception:
            pass

    @property
    def job_id(self):
        return self._job_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_command(
        self,
        datastore_type,
        code_package_metadata,
        code_package_url,
        step_name,
        step_cmds,
        task_spec,
    ):
        """Construct the bash command that runs inside the container.

        Follows the same three-phase pattern used by the Batch and Kubernetes
        backends:
          1. Export mflog env vars (structured logging setup)
          2. Download + extract the code package
          3. Bootstrap the Python environment, then run the step
        """
        from metaflow.mflog import (
            BASH_SAVE_LOGS,
            bash_capture_logs,
            export_mflog_env_vars,
        )

        mflog_expr = export_mflog_env_vars(
            datastore_type=datastore_type,
            stdout_path=_STDOUT_PATH,
            stderr_path=_STDERR_PATH,
            **task_spec,
        )
        init_cmds = self.environment.get_package_commands(
            code_package_url, datastore_type, code_package_metadata
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self.environment.bootstrap_commands(step_name, datastore_type)
                + step_cmds
            )
        )

        # true — compatible with containers that have ENTRYPOINT set to eval $@
        cmd_str = "true && mkdir -p {logs} && {mflog} && {init} && {step}".format(
            logs=_LOGS_DIR,
            mflog=mflog_expr,
            init=init_expr,
            step=step_expr,
        )
        # Persist final logs regardless of step exit code, then re-raise it.
        cmd_str = "%s; c=$?; %s; exit $c" % (cmd_str, BASH_SAVE_LOGS)

        return shlex.split('bash -c "%s"' % cmd_str)

    def _build_env(
        self,
        task_spec,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        datastore_type,
        extra,
    ):
        """Assemble the full environment variable dict for the container."""
        from metaflow import util
        from metaflow.metaflow_config import (
            CARD_S3ROOT,
            DATASTORE_SYSROOT_S3,
            DATATOOLS_S3ROOT,
            DEFAULT_METADATA,
            OTEL_ENDPOINT,
            S3_ENDPOINT_URL,
            SERVICE_HEADERS,
            SERVICE_INTERNAL_URL,
        )

        env = {
            # Code package
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_METADATA": code_package_metadata,
            "METAFLOW_CODE_DS": datastore_type,
            # Datastore
            "METAFLOW_DEFAULT_DATASTORE": datastore_type,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            # Identity
            "METAFLOW_USER": util.get_username(),
            "METAFLOW_RUNTIME_ENVIRONMENT": "bacalhau",
            "METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0]),
        }

        # S3-specific configuration
        if datastore_type == "s3":
            if DATASTORE_SYSROOT_S3:
                env["METAFLOW_DATASTORE_SYSROOT_S3"] = DATASTORE_SYSROOT_S3
            if DATATOOLS_S3ROOT:
                env["METAFLOW_DATATOOLS_S3ROOT"] = DATATOOLS_S3ROOT
            if CARD_S3ROOT:
                env["METAFLOW_CARD_S3ROOT"] = CARD_S3ROOT
            # Allow an override URL for inside-container use (e.g. host.docker.internal
            # when MinIO is exposed on the Docker host rather than inside the cluster).
            container_s3_url = os.environ.get(
                "METAFLOW_BACALHAU_CONTAINER_S3_ENDPOINT_URL", S3_ENDPOINT_URL
            )
            if container_s3_url:
                env["METAFLOW_S3_ENDPOINT_URL"] = container_s3_url
            # Forward AWS credentials so the container can access S3.
            for aws_var in (
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_DEFAULT_REGION",
                "AWS_REGION",
            ):
                val = os.environ.get(aws_var)
                if val:
                    env[aws_var] = val

        # Metaflow service (metadata/UI)
        if SERVICE_INTERNAL_URL:
            env["METAFLOW_SERVICE_INTERNAL_URL"] = SERVICE_INTERNAL_URL
        if SERVICE_HEADERS:
            import json

            env["METAFLOW_SERVICE_HEADERS"] = json.dumps(SERVICE_HEADERS)
        if OTEL_ENDPOINT:
            env["METAFLOW_OTEL_ENDPOINT"] = OTEL_ENDPOINT

        # Caller-supplied extras (e.g. from @environment decorator) win.
        env.update(extra)
        return env

    def _wait(self, jobs_client, job_id, timeout):
        """Poll Bacalhau until the job reaches a terminal state."""
        deadline = time.time() + timeout if timeout else None
        last_state = None

        while True:
            resp = jobs_client.executions(job_id, limit=10)
            executions = (resp.executions or []) if resp else []

            for execution in executions:
                state = self._exec_state(execution)
                if state != last_state:
                    last_state = state

                if state in _TERMINAL_STATES:
                    if state in _FAILED_STATES:
                        msg = self._failure_message(execution)
                        raise BacalhauException(
                            "Bacalhau job %s failed: %s" % (job_id, msg)
                        )
                    # Completed successfully.
                    return execution

            if deadline and time.time() > deadline:
                self.kill()
                raise BacalhauException(
                    "Bacalhau job %s timed out after %d seconds" % (job_id, timeout)
                )

            time.sleep(_POLL_INTERVAL)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _exec_state(execution):
        """Return the compute state string (e.g. '9' for Completed)."""
        if execution.compute_state and execution.compute_state.state_type is not None:
            return str(execution.compute_state.state_type)
        return "0"

    @staticmethod
    def _failure_message(execution):
        parts = []
        if execution.compute_state and execution.compute_state.message:
            parts.append(execution.compute_state.message)
        if execution.run_output:
            if execution.run_output.error_msg:
                parts.append(execution.run_output.error_msg)
            if execution.run_output.stderr:
                parts.append("stderr: " + execution.run_output.stderr[:2000])
        return " | ".join(parts) if parts else "unknown error"

    @staticmethod
    def _normalise_memory(memory):
        """Convert Metaflow's MB integer to a Bacalhau memory string."""
        if memory is None:
            return "4096Mb"
        try:
            return "%dMb" % int(memory)
        except (TypeError, ValueError):
            # Already a string like "4Gi" — pass through.
            return str(memory)
