"""Unit tests for BacalhauJob.

These tests run against the real Metaflow (must be installed) and mock only
the Bacalhau SDK, which may not be installed in CI.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Ensure the package root is on the path so local source is found first.
# ---------------------------------------------------------------------------
_PKG_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)


from metaflow_extensions.bacalhau.plugins.bacalhau_exceptions import (  # noqa: E402
    BacalhauException,
)
from metaflow_extensions.bacalhau.plugins.bacalhau_job import (  # noqa: E402
    BacalhauJob,
    _EXEC_CANCELLED,
    _EXEC_COMPLETED,
    _EXEC_FAILED,
)


class TestBacalhauJobHelpers(unittest.TestCase):
    """Tests for static helper methods — no SDK needed."""

    def _make_execution(self, state_type, message=None, run_output=None):
        ex = MagicMock()
        ex.compute_state.state_type = state_type
        ex.compute_state.message = message
        ex.run_output = run_output
        return ex

    def test_exec_state_completed(self):
        self.assertEqual(
            BacalhauJob._exec_state(self._make_execution(_EXEC_COMPLETED)),
            _EXEC_COMPLETED,
        )

    def test_exec_state_failed(self):
        self.assertEqual(
            BacalhauJob._exec_state(self._make_execution(_EXEC_FAILED)),
            _EXEC_FAILED,
        )

    def test_exec_state_none_compute_state(self):
        ex = MagicMock()
        ex.compute_state = None
        self.assertEqual(BacalhauJob._exec_state(ex), "0")

    def test_failure_message_uses_compute_state_message(self):
        ex = self._make_execution(_EXEC_FAILED, message="OOM killed", run_output=None)
        self.assertIn("OOM killed", BacalhauJob._failure_message(ex))

    def test_failure_message_includes_stderr(self):
        ro = MagicMock()
        ro.error_msg = ""
        ro.stderr = "Traceback: something went wrong"
        ex = self._make_execution(_EXEC_FAILED, message="", run_output=ro)
        self.assertIn("Traceback", BacalhauJob._failure_message(ex))

    def test_normalise_memory_integer_mb(self):
        self.assertEqual(BacalhauJob._normalise_memory("4096"), "4096Mb")

    def test_normalise_memory_none(self):
        self.assertEqual(BacalhauJob._normalise_memory(None), "4096Mb")

    def test_normalise_memory_passthrough_gi_string(self):
        self.assertEqual(BacalhauJob._normalise_memory("4Gi"), "4Gi")


class TestBacalhauJobWait(unittest.TestCase):
    """Tests for the polling / wait logic."""

    def _make_job(self):
        return BacalhauJob(MagicMock(), MagicMock())

    def _make_execution(self, state_type):
        ex = MagicMock()
        ex.compute_state.state_type = state_type
        ex.compute_state.message = ""
        ex.run_output = None
        return ex

    def test_returns_on_completed(self):
        job = self._make_job()
        client = MagicMock()
        completed = self._make_execution(_EXEC_COMPLETED)
        client.executions.return_value.executions = [completed]

        result = job._wait(client, "job-123", timeout=60)
        self.assertIs(result, completed)

    def test_raises_on_failed(self):
        job = self._make_job()
        client = MagicMock()
        client.executions.return_value.executions = [
            self._make_execution(_EXEC_FAILED)
        ]
        with self.assertRaises(BacalhauException):
            job._wait(client, "job-123", timeout=60)

    def test_raises_on_cancelled(self):
        job = self._make_job()
        client = MagicMock()
        client.executions.return_value.executions = [
            self._make_execution(_EXEC_CANCELLED)
        ]
        with self.assertRaises(BacalhauException):
            job._wait(client, "job-123", timeout=60)

    def test_polls_multiple_times_before_terminal(self):
        job = self._make_job()
        client = MagicMock()
        running = self._make_execution("6")
        completed = self._make_execution(_EXEC_COMPLETED)
        client.executions.side_effect = [
            MagicMock(executions=[running]),
            MagicMock(executions=[running]),
            MagicMock(executions=[completed]),
        ]

        with patch(
            "metaflow_extensions.bacalhau.plugins.bacalhau_job.time.sleep"
        ) as mock_sleep:
            result = job._wait(client, "job-123", timeout=3600)

        self.assertEqual(client.executions.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        self.assertIs(result, completed)

    def test_timeout_raises_and_kills(self):
        job = self._make_job()
        job._job_id = "job-123"
        client = MagicMock()
        client.executions.return_value.executions = [self._make_execution("6")]

        with patch(
            "metaflow_extensions.bacalhau.plugins.bacalhau_job.time.sleep"
        ):
            with patch(
                "metaflow_extensions.bacalhau.plugins.bacalhau_job.time.time"
            ) as mock_time:
                mock_time.side_effect = [0, 0, 9999]
                with self.assertRaises(BacalhauException) as ctx:
                    job._wait(client, "job-123", timeout=5)

        self.assertIn("timed out", str(ctx.exception))

    def test_empty_executions_list_keeps_polling(self):
        job = self._make_job()
        client = MagicMock()
        completed = self._make_execution(_EXEC_COMPLETED)
        client.executions.side_effect = [
            MagicMock(executions=[]),
            MagicMock(executions=[completed]),
        ]

        with patch(
            "metaflow_extensions.bacalhau.plugins.bacalhau_job.time.sleep"
        ):
            result = job._wait(client, "job-123", timeout=60)
        self.assertIs(result, completed)


class TestBacalhauJobBuildEnv(unittest.TestCase):
    """Tests for _build_env environment variable construction."""

    _TASK_SPEC = {
        "flow_name": "TestFlow",
        "step_name": "start",
        "run_id": "1",
        "task_id": "2",
        "retry_count": "0",
    }

    def _make_job(self):
        return BacalhauJob(MagicMock(), MagicMock())

    def test_always_includes_code_url(self):
        job = self._make_job()
        with patch.dict(os.environ, {}, clear=False):
            result = job._build_env(
                task_spec=self._TASK_SPEC,
                code_package_metadata="meta",
                code_package_sha="sha256",
                code_package_url="s3://bucket/code.tar",
                datastore_type="s3",
                extra={},
            )
        self.assertEqual(result["METAFLOW_CODE_URL"], "s3://bucket/code.tar")
        self.assertEqual(result["METAFLOW_CODE_SHA"], "sha256")
        self.assertEqual(result["METAFLOW_DEFAULT_DATASTORE"], "s3")

    def test_aws_credentials_forwarded(self):
        job = self._make_job()
        with patch.dict(
            os.environ,
            {"AWS_ACCESS_KEY_ID": "AKID", "AWS_SECRET_ACCESS_KEY": "SECRET"},
        ):
            result = job._build_env(
                task_spec=self._TASK_SPEC,
                code_package_metadata="meta",
                code_package_sha="sha",
                code_package_url="s3://b/code.tar",
                datastore_type="s3",
                extra={},
            )
        self.assertEqual(result.get("AWS_ACCESS_KEY_ID"), "AKID")
        self.assertEqual(result.get("AWS_SECRET_ACCESS_KEY"), "SECRET")

    def test_aws_credentials_not_included_when_unset(self):
        job = self._make_job()
        env_without_aws = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("AWS_")
        }
        with patch.dict(os.environ, env_without_aws, clear=True):
            result = job._build_env(
                task_spec=self._TASK_SPEC,
                code_package_metadata="meta",
                code_package_sha="sha",
                code_package_url="s3://b/code.tar",
                datastore_type="s3",
                extra={},
            )
        self.assertNotIn("AWS_ACCESS_KEY_ID", result)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", result)

    def test_extra_env_overrides_defaults(self):
        job = self._make_job()
        result = job._build_env(
            task_spec=self._TASK_SPEC,
            code_package_metadata="meta",
            code_package_sha="sha",
            code_package_url="s3://b/code.tar",
            datastore_type="s3",
            extra={"MY_VAR": "hello", "METAFLOW_USER": "override"},
        )
        self.assertEqual(result["MY_VAR"], "hello")
        self.assertEqual(result["METAFLOW_USER"], "override")

    def test_runtime_environment_set_to_bacalhau(self):
        job = self._make_job()
        result = job._build_env(
            task_spec=self._TASK_SPEC,
            code_package_metadata="meta",
            code_package_sha="sha",
            code_package_url="s3://b/code.tar",
            datastore_type="s3",
            extra={},
        )
        self.assertEqual(result["METAFLOW_RUNTIME_ENVIRONMENT"], "bacalhau")


if __name__ == "__main__":
    unittest.main()
