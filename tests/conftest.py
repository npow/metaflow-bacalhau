"""
pytest configuration for metaflow-bacalhau tests.

Metaflow must be fully initialised before any test module imports our
extension decorators directly.  Importing ``metaflow`` here (before pytest
collects test modules) ensures that:

  1. Metaflow's plugin registry loads our extension in the correct order.
  2. ``metaflow.decorators.StepDecorator`` is available when
     ``bacalhau_decorator.py`` is imported, avoiding a circular-import error
     that would otherwise occur when the extension is the *first* code to
     trigger a ``metaflow`` import.
"""

import metaflow  # noqa: F401 — side-effect import, must run before test collection
