"""
hello_bacalhau.py — minimal example of running Metaflow steps on Bacalhau.

Prerequisites
-------------
1. Install dependencies:
       pip install metaflow-bacalhau

2. Configure Bacalhau API endpoint (defaults to localhost:1234):
       export BACALHAU_API_HOST=<your-bacalhau-node>
       export BACALHAU_API_PORT=1234

3. Configure an S3 datastore (required by the Bacalhau backend):
       export AWS_ACCESS_KEY_ID=...
       export AWS_SECRET_ACCESS_KEY=...
       export METAFLOW_DATASTORE_SYSROOT_S3=s3://my-bucket/metaflow

4. Run:
       python hello_bacalhau.py --datastore=s3 run
"""

from metaflow import FlowSpec, Parameter, step


class HelloBacalhauFlow(FlowSpec):
    """A simple flow that runs two steps on Bacalhau."""

    message = Parameter(
        "message",
        default="Hello from Bacalhau!",
        help="Message to echo in the remote step.",
    )

    @step
    def start(self):
        print("Starting flow locally.")
        self.next(self.process)

    # Import the decorator inside the class so the file can be imported
    # without metaflow-bacalhau installed (e.g. during linting).
    try:
        from metaflow import bacalhau  # type: ignore[attr-defined]

        @bacalhau(cpu=1, memory=512, image="python:3.11-slim")
        @step
        def process(self):
            """This step runs on the Bacalhau network."""
            import platform
            import socket

            print("Message:", self.message)
            print("Python:", platform.python_version())
            print("Hostname:", socket.gethostname())
            self.result = "Processed: " + self.message
            self.next(self.end)

    except ImportError:
        # Fallback: run locally when the Bacalhau extension is not installed.
        @step
        def process(self):
            self.result = "Processed (local): " + self.message
            self.next(self.end)

    @step
    def end(self):
        print("Result:", self.result)


if __name__ == "__main__":
    HelloBacalhauFlow()
