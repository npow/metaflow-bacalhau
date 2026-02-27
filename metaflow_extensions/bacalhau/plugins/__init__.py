# Register this extension's plugins with Metaflow's extension system.
# Metaflow discovers these via the metaflow_extensions namespace package mechanism.

STEP_DECORATORS_DESC = [
    ("bacalhau", ".bacalhau_decorator.BacalhauDecorator"),
]

CLIS_DESC = [
    ("bacalhau", ".bacalhau_cli.cli"),
]
