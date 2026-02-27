"""
e2e_test.py — end-to-end test flow for the Bacalhau Metaflow backend.

Runs a simple step on Bacalhau and verifies the result comes back correctly.

Usage (after sourcing dev/env.sh):
    python examples/e2e_test.py run
"""

from metaflow import FlowSpec, step
from metaflow import bacalhau  # noqa: F401 — registered by our extension


class E2ETestFlow(FlowSpec):

    @step
    def start(self):
        self.greeting = "hello from the orchestrator"
        self.next(self.remote_step)

    @bacalhau(
        cpu=1,
        memory=256,
        image="metaflow-bacalhau-dev:latest",
    )
    @step
    def remote_step(self):
        """Runs inside a Bacalhau container on the devstack."""
        import os
        import platform
        import socket

        print("=== remote_step running inside Bacalhau ===")
        print("hostname     :", socket.gethostname())
        print("python       :", platform.python_version())
        print("BACALHAU_JOB_ID:", os.environ.get("BACALHAU_JOB_ID", "<not set>"))
        print("greeting in :", self.greeting)

        self.result = "remote processed: " + self.greeting
        self.remote_hostname = socket.gethostname()
        self.next(self.end)

    @step
    def end(self):
        print("=== end step (local) ===")
        print("result       :", self.result)
        print("remote host  :", self.remote_hostname)
        assert self.result.startswith("remote processed:"), "Unexpected result: " + self.result
        print("E2E TEST PASSED")


if __name__ == "__main__":
    E2ETestFlow()
