from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import tempfile
from types import ModuleType
import unittest
from unittest import TestCase
from unittest.mock import patch


_has_setuptools = importlib.util.find_spec("setuptools") is not None


_SETUP_PATH = Path(__file__).parents[1] / "setup.py"
setup_module: ModuleType | None = None
if _has_setuptools:
    _SPEC = importlib.util.spec_from_file_location(
        "framework_shells_setup_test",
        _SETUP_PATH,
    )
    if _SPEC is None or _SPEC.loader is None:
        raise RuntimeError(f"Unable to load setup helpers from {_SETUP_PATH}")
    setup_module = importlib.util.module_from_spec(_SPEC)
    with patch("setuptools.setup"):
        _SPEC.loader.exec_module(setup_module)


def _loaded_setup_module() -> ModuleType:
    if setup_module is None:
        raise AssertionError("setuptools is required to inspect setup.py")
    return setup_module


@unittest.skipUnless(_has_setuptools, "setuptools is required to inspect setup.py")
class PipePumpPyo3ConfigTests(TestCase):
    def test_static_free_threaded_python_uses_explicit_no_link_config(self) -> None:
        module = _loaded_setup_module()
        config_path = Path("/candidate/pyo3-config.txt")
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(module, "_is_free_threaded_python", return_value=True),
            patch.object(module, "_is_android_python", return_value=False),
            patch.object(module, "_is_shared_python", return_value=False),
            patch.object(
                module,
                "_write_pipe_pump_pyo3_config",
                return_value=config_path,
            ) as write_config,
        ):
            env = module._pipe_pump_cargo_env()

        self.assertEqual(env["PYO3_BUILD_EXTENSION_MODULE"], "1")
        self.assertEqual(env["PYO3_CONFIG_FILE"], str(config_path))
        write_config.assert_called_once_with()

    def test_shared_free_threaded_linux_python_uses_normal_pyo3_probe(self) -> None:
        module = _loaded_setup_module()
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(module, "_is_free_threaded_python", return_value=True),
            patch.object(module, "_is_android_python", return_value=False),
            patch.object(module, "_is_shared_python", return_value=True),
            patch.object(module, "_write_pipe_pump_pyo3_config") as write_config,
        ):
            env = module._pipe_pump_cargo_env()

        self.assertNotIn("PYO3_CONFIG_FILE", env)
        write_config.assert_not_called()

    def test_generated_config_suppresses_python_link_lines(self) -> None:
        module = _loaded_setup_module()
        with tempfile.TemporaryDirectory() as temp_root:
            with patch.object(module, "ROOT", Path(temp_root)):
                config_path = module._write_pipe_pump_pyo3_config()

            config = config_path.read_text(encoding="utf-8")

        self.assertIn("shared=false\n", config)
        self.assertIn("abi3=false\n", config)
        self.assertIn("build_flags=Py_GIL_DISABLED\n", config)
        self.assertIn("suppress_build_script_link_lines=true\n", config)


if __name__ == "__main__":
    unittest.main()
