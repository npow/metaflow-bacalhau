"""
Exception classes for the Bacalhau Metaflow backend.

Defined with NO module-level imports so this file can be safely imported
during Metaflow's own plugin-loading phase without triggering circular
imports.  MetaflowException is wired in lazily by bacalhau_job.py once
Metaflow has fully initialised.
"""


class BacalhauException(Exception):
    """Raised when a Bacalhau job submission or monitoring step fails."""

    headline = "Bacalhau error"


class BacalhauKilledException(Exception):
    """Raised when a Bacalhau job is killed before it completes."""

    headline = "Bacalhau task killed"


def _upgrade_bases():
    """Re-parent exception classes to MetaflowException once Metaflow is live.

    Call this from bacalhau_job.py at the top of any method that submits or
    monitors a job (i.e. after Metaflow has fully initialised).
    """
    import sys

    if "metaflow.exception" not in sys.modules:
        return  # Not yet available; skip silently.

    from metaflow.exception import MetaflowException

    for cls in (BacalhauException, BacalhauKilledException):
        if not issubclass(cls, MetaflowException):
            cls.__bases__ = (MetaflowException,)
