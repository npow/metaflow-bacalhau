"""Unit tests for BacalhauDecorator.

Runs against the real Metaflow installation (must be present).
"""

import os
import platform
import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure our local source tree takes precedence over any installed copy.
# ---------------------------------------------------------------------------
_PKG_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

from metaflow_extensions.bacalhau.plugins.bacalhau_decorator import (  # noqa: E402
    BacalhauDecorator,
    _compute_resource_attributes,
)
from metaflow_extensions.bacalhau.plugins.bacalhau_exceptions import (  # noqa: E402
    BacalhauException,
)

_IMAGE_MODULE = "metaflow_extensions.bacalhau.plugins.bacalhau_decorator"


def _make_deco(**attrs):
    """Instantiate a BacalhauDecorator bypassing __init__ validation."""
    d = BacalhauDecorator.__new__(BacalhauDecorator)
    d.attributes = dict(BacalhauDecorator.defaults)
    d.attributes.update(attrs)
    return d


class TestBacalhauDecoratorInit(unittest.TestCase):
    """Tests for the init() attribute-defaulting and validation logic."""

    def test_default_image_matches_local_python(self):
        deco = _make_deco()
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            deco.init()
        major, minor, _ = platform.python_version_tuple()
        self.assertEqual(deco.attributes["image"], "python:%s.%s" % (major, minor))

    def test_env_var_overrides_default_image(self):
        deco = _make_deco()
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", "myorg/ml:v2"):
            deco.init()
        self.assertEqual(deco.attributes["image"], "myorg/ml:v2")

    def test_explicit_image_passthrough(self):
        deco = _make_deco(image="custom:tag")
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            deco.init()
        self.assertEqual(deco.attributes["image"], "custom:tag")

    def test_registry_prepended_when_no_slash(self):
        deco = _make_deco(image="myimage:v1")
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_REGISTRY", "r.io"):
                deco.init()
        self.assertEqual(deco.attributes["image"], "r.io/myimage:v1")

    def test_registry_not_prepended_when_slash_present(self):
        deco = _make_deco(image="r.io/myimage:v1")
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            with patch(
                _IMAGE_MODULE + "._BACALHAU_CONTAINER_REGISTRY", "other.registry.io"
            ):
                deco.init()
        self.assertEqual(deco.attributes["image"], "r.io/myimage:v1")

    def test_registry_trailing_slash_stripped(self):
        deco = _make_deco(image="img:v1")
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_REGISTRY", "r.io/"):
                deco.init()
        self.assertEqual(deco.attributes["image"], "r.io/img:v1")

    def test_labels_default_to_empty_dict(self):
        deco = _make_deco()
        with patch(_IMAGE_MODULE + "._BACALHAU_CONTAINER_IMAGE", ""):
            deco.init()
        self.assertIsInstance(deco.attributes["labels"], dict)
        self.assertEqual(len(deco.attributes["labels"]), 0)


class TestStepInit(unittest.TestCase):
    """Tests for the step_init() validation logic."""

    def _run_step_init(self, deco, datastore_type="s3"):
        fake_ds = MagicMock()
        fake_ds.TYPE = datastore_type
        deco.step_init(
            flow=MagicMock(),
            graph=MagicMock(),
            step="start",
            decos=[],
            environment=MagicMock(),
            flow_datastore=fake_ds,
            logger=MagicMock(),
        )
        return deco

    def test_rejects_non_s3_datastore(self):
        deco = _make_deco(image="python:3.11")
        with self.assertRaises(BacalhauException):
            self._run_step_init(deco, datastore_type="local")

    def test_accepts_s3_datastore(self):
        deco = _make_deco(image="python:3.11")
        deco = self._run_step_init(deco, datastore_type="s3")
        self.assertEqual(deco.flow_datastore.TYPE, "s3")

    def test_stores_environment_reference(self):
        deco = _make_deco(image="python:3.11")
        deco = self._run_step_init(deco)
        self.assertIsNotNone(deco.environment)

    def test_timeout_falls_back_to_run_time_limit(self):
        deco = _make_deco(image="python:3.11", timeout=None)
        with patch(
            "metaflow_extensions.bacalhau.plugins.bacalhau_decorator"
            ".get_run_time_limit_for_task",
            return_value=3600,
        ):
            deco = self._run_step_init(deco)
        self.assertEqual(deco.attributes["timeout"], 3600)


class TestComputeResourceAttributes(unittest.TestCase):
    """Tests for the _compute_resource_attributes helper."""

    def _fake_deco(self, name, **attrs):
        d = MagicMock()
        d.name = name
        d.attributes = attrs
        return d

    def test_defaults_used_when_no_resources_deco(self):
        deco = MagicMock()
        deco.attributes = {"cpu": None, "gpu": None, "memory": None}
        result = _compute_resource_attributes(
            [], deco, {"cpu": "1", "gpu": "0", "memory": "4096"}
        )
        self.assertEqual(result, {"cpu": "1", "gpu": "0", "memory": "4096"})

    def test_resources_deco_wins_over_defaults(self):
        res = self._fake_deco("resources", cpu=4, gpu=0, memory=8192)
        deco = MagicMock()
        deco.attributes = {"cpu": None, "gpu": None, "memory": None}
        result = _compute_resource_attributes(
            [res], deco, {"cpu": "1", "gpu": "0", "memory": "4096"}
        )
        self.assertEqual(result["cpu"], "4")
        self.assertEqual(result["memory"], "8192")

    def test_own_attributes_win_over_resources_deco(self):
        res = self._fake_deco("resources", cpu=2, memory=4096)
        deco = MagicMock()
        deco.attributes = {"cpu": "8", "gpu": None, "memory": None}
        result = _compute_resource_attributes(
            [res], deco, {"cpu": "1", "gpu": "0", "memory": "4096"}
        )
        self.assertEqual(result["cpu"], "8")

    def test_maximum_of_resources_and_own(self):
        res = self._fake_deco("resources", cpu=8, memory=16384)
        deco = MagicMock()
        deco.attributes = {"cpu": "4", "gpu": None, "memory": "32768"}
        result = _compute_resource_attributes(
            [res], deco, {"cpu": "1", "gpu": "0", "memory": "4096"}
        )
        self.assertEqual(result["cpu"], "8")    # resources deco has more
        self.assertEqual(result["memory"], "32768")  # own attr has more


class TestRuntimeStepCli(unittest.TestCase):
    """Tests for the runtime_step_cli method."""

    def test_commands_redirected_to_bacalhau_step(self):
        deco = _make_deco(image="python:3.11", cpu="2", memory="4096", gpu="0")
        # Set class-level cache as if a package was already uploaded.
        BacalhauDecorator.package_url = "s3://bucket/pkg.tar"
        BacalhauDecorator.package_sha = "deadbeef"
        BacalhauDecorator.package_metadata = '{"version":"1"}'

        cli_args = MagicMock()
        cli_args.command_args = []
        cli_args.command_options = {}
        cli_args.entrypoint = ["/some/python"]

        deco.runtime_step_cli(cli_args, retry_count=0, max_user_code_retries=3,
                              ubf_context=None)

        self.assertEqual(cli_args.commands, ["bacalhau", "step"])
        self.assertIn("s3://bucket/pkg.tar", cli_args.command_args)
        self.assertIn("deadbeef", cli_args.command_args)

    def test_commands_not_redirected_after_max_retries(self):
        deco = _make_deco(image="python:3.11")
        cli_args = MagicMock()
        cli_args.commands = ["step"]

        deco.runtime_step_cli(cli_args, retry_count=2, max_user_code_retries=1,
                              ubf_context=None)

        # Should not have been overwritten.
        self.assertEqual(cli_args.commands, ["step"])


if __name__ == "__main__":
    unittest.main()
