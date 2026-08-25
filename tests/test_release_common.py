from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


_COMMON_PATH = Path(__file__).parents[1] / "scripts" / "release" / "common.py"
_SPEC = importlib.util.spec_from_file_location("fws_release_common", _COMMON_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load release helpers from {_COMMON_PATH}")
common = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(common)


class TerminalBrokerBinaryPathTests(TestCase):
    def test_uses_manifest_local_target_without_override(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            path = common.terminal_broker_binary_path(
                target="x86_64-unknown-linux-gnu",
                profile="release",
            )

        self.assertEqual(
            path,
            common.ROOT
            / "native"
            / "fws_terminal_stream_broker"
            / "target"
            / "x86_64-unknown-linux-gnu"
            / "release"
            / common.TERMINAL_BROKER_BIN_NAME,
        )

    def test_uses_cargo_target_dir_override(self) -> None:
        configured = Path("~/.cache/fws-cargo").expanduser()
        with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(configured)}, clear=True):
            path = common.terminal_broker_binary_path(
                target="aarch64-linux-android",
                profile="debug",
            )

        self.assertEqual(
            path,
            configured
            / "aarch64-linux-android"
            / "debug"
            / common.TERMINAL_BROKER_BIN_NAME,
        )
