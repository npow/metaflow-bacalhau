"""
CLI commands for the Bacalhau Metaflow backend.

The 'bacalhau step' command is the entry point invoked by Metaflow's runtime
when a step is decorated with @bacalhau.  It is NOT meant to be called by
users directly; Metaflow's orchestrator calls it via runtime_step_cli().
"""

import os
import sys
import traceback

from metaflow import util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY, CommandException
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.mflog import TASK_LOG_SOURCE

from .bacalhau_exceptions import BacalhauException
from .bacalhau_job import BacalhauJob


@click.group()
def cli():
    pass


@cli.group(help="Commands related to the Bacalhau compute backend.")
def bacalhau():
    pass


# ── 'bacalhau step' — internal, called by Metaflow runtime ────────────────────


@bacalhau.command(
    help=(
        "Execute a single Metaflow step on the Bacalhau network.  "
        "This command is invoked internally by Metaflow; do not call it directly."
    )
)
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--image", help="Docker image to use.")
@click.option("--cpu", default=None, help="CPU requirement (e.g. '2' or '500m').")
@click.option("--gpu", default=None, help="Number of GPUs.")
@click.option("--memory", default=None, help="Memory in MB.")
@click.option("--timeout", default=None, type=int, help="Job timeout in seconds.")
@click.option("--labels", default=None, help="JSON string of Bacalhau job labels.")
# ── Standard Metaflow step routing options ─────────────────────────────────────
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option(
    "--tag", multiple=True, default=None, help="Passed to the top-level 'step'."
)
@click.option("--namespace", default=None, help="Passed to the top-level 'step'.")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option(
    "--max-user-code-retries", default=0, help="Passed to the top-level 'step'."
)
@click.option("--run-time-limit", default=None, type=int)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_metadata,
    code_package_sha,
    code_package_url,
    image=None,
    cpu=None,
    gpu=None,
    memory=None,
    timeout=None,
    labels=None,
    run_time_limit=None,
    **kwargs,
):
    def echo(msg, stream="stderr", job_id=None, **_kw):
        msg = util.to_unicode(msg)
        if job_id:
            msg = "[%s] %s" % (job_id, msg)
        ctx.obj.echo_always(msg, err=(stream == "stderr"), **_kw)

    # ── Build the step CLI command that will run *inside* the container ────────
    executable = ctx.obj.environment.executable(step_name)
    entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))
    top_args = " ".join(util.dict_to_cli_options(ctx.parent.parent.params))

    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            "METAFLOW_INPUT_PATHS_%d" % (i // max_size): input_paths[i : i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join("${%s}" % s for s in split_vars.keys())

    step_args = " ".join(util.dict_to_cli_options(kwargs))
    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint=entrypoint,
        top_args=top_args,
        step=step_name,
        step_args=step_args,
    )

    # ── Build task identification metadata ────────────────────────────────────
    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "step_name": step_name,
        "run_id": kwargs["run_id"],
        "task_id": kwargs["task_id"],
        "retry_count": str(kwargs.get("retry_count", 0)),
    }
    attrs = {"metaflow.%s" % k: v for k, v in task_spec.items()}
    attrs["metaflow.user"] = util.get_username()
    attrs["metaflow.version"] = ctx.obj.environment.get_environment_info()[
        "metaflow_version"
    ]

    # ── Collect extra environment variables ───────────────────────────────────
    env = {"METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0])}

    # Variables from @environment decorator on this step.
    node = ctx.obj.graph[step_name]
    env_deco = [d for d in node.decorators if d.name == "environment"]
    if env_deco:
        env.update(env_deco[0].attributes["vars"])

    # Propagate split input-path fragments.
    if split_vars:
        env.update(split_vars)

    # ── Resolve effective timeout ─────────────────────────────────────────────
    effective_timeout = timeout or run_time_limit or (5 * 24 * 60 * 60)

    # ── Parse labels from JSON string ─────────────────────────────────────────
    parsed_labels = {}
    if labels:
        import json

        try:
            parsed_labels = json.loads(labels)
        except (ValueError, TypeError):
            pass
    parsed_labels.update(
        {
            "metaflow.flow": ctx.obj.flow.name,
            "metaflow.step": step_name,
            "metaflow.run_id": str(kwargs.get("run_id", "")),
        }
    )

    # ── Open the task datastore so Metaflow can tail logs ─────────────────────
    ds = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
        attempt=int(kwargs.get("retry_count", 0)),
    )

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, kwargs["task_id"]
                ),
            )

    # ── Submit job ────────────────────────────────────────────────────────────
    job = BacalhauJob(ctx.obj.metadata, ctx.obj.environment)
    try:
        job.launch_job(
            step_name=step_name,
            step_cli=step_cli,
            task_spec=task_spec,
            code_package_metadata=code_package_metadata,
            code_package_sha=code_package_sha,
            code_package_url=code_package_url,
            datastore_type=ctx.obj.flow_datastore.TYPE,
            image=image,
            cpu=cpu,
            gpu=gpu,
            memory=memory,
            timeout=effective_timeout,
            env=env,
            labels=parsed_labels,
        )
        echo("Bacalhau job %s completed." % job.job_id, job_id=job.job_id)
    except BacalhauException as ex:
        echo(str(ex), stream="stderr", job_id=job.job_id)
        _sync_metadata()
        sys.exit(1)
    except Exception:
        echo(traceback.format_exc(), stream="stderr", job_id=job.job_id)
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    else:
        _sync_metadata()


# ── 'bacalhau kill' — convenience command for operators ───────────────────────


@bacalhau.command(help="Terminate unfinished Bacalhau tasks for this flow.")
@click.option("--job-id", required=True, help="Bacalhau job ID to kill.")
@click.pass_context
def kill(ctx, job_id):
    """Kill a specific Bacalhau job by ID."""
    from bacalhau_sdk.jobs import Jobs

    try:
        Jobs().stop(job_id, reason="Killed by Metaflow operator")
        ctx.obj.echo("Job %s stopped." % job_id)
    except Exception as ex:
        raise CommandException("Failed to stop job %s: %s" % (job_id, ex))
