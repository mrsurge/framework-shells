from __future__ import annotations

import argparse
import os
from pathlib import Path
import tempfile
import textwrap
import venv

from common import run, venv_python_path


_NATIVE_SMOKE = r"""
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from framework_shells.manager import FrameworkShellManager
from framework_shells.native_pipe import resolve_native_terminal_broker_command
from framework_shells.orchestrator import Orchestrator
from framework_shells.store import RuntimeStore

resolution = resolve_native_terminal_broker_command(["fallback"])
assert resolution.engine == "native-terminal-pipe", resolution
assert "framework_shells/bin/fws-terminal-stream-broker" in (resolution.source or ""), resolution

SPEC_TEXT = '''version: "1"
shells:
  terminal:
    backend: pipe
    pipe:
      mode: native_terminal_pipe_testing
    cwd: ${ctx:CWD}
    env:
      TERMINAL_STREAM_CWD: ${ctx:CWD}
      TERMINAL_STREAM_COLS: ${ctx:COLS}
      TERMINAL_STREAM_ROWS: ${ctx:ROWS}
      TERMINAL_STREAM_SHELL_CMD_JSON: ${ctx:SHELL_CMD_JSON}
'''

async def main() -> None:
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        spec_path = tmp_path / "terminal.yaml"
        spec_path.write_text(SPEC_TEXT, encoding="utf-8")
        store = RuntimeStore(tmp_path / "runtime")
        mgr = FrameworkShellManager(store=store)
        orch = Orchestrator(mgr)
        record = await orch.start_from_ref(
            "terminal.yaml#terminal",
            base_dir=tmp_path,
            ctx={
                "CWD": str(tmp_path),
                "COLS": "111",
                "ROWS": "42",
                "SHELL_CMD_JSON": json.dumps(["sh", "-lc", "printf hello; exit 0"]),
            },
            label="wheel-smoke-terminal",
            wait_ready=False,
        )
        queue = await mgr.subscribe_output_bytes(record.id)
        await mgr.write_to_pipe(
            record.id,
            json.dumps({"jsonrpc": "2.0", "method": "terminal.connect", "params": {}}) + "\n",
        )
        parts: list[str] = []
        for _ in range(12):
            chunk = await asyncio.wait_for(queue.get(), timeout=5)
            if chunk is None:
                break
            text = chunk.decode("utf-8", errors="replace")
            parts.append(text)
            if '"type":"closed"' in text.replace(" ", ""):
                break
        blob = "".join(parts)
        assert '"type":"ready"' in blob, blob
        assert '"type":"data"' in blob, blob
        assert '"type":"closed"' in blob, blob

asyncio.run(main())
"""


_PURE_SMOKE = r"""
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from framework_shells.manager import FrameworkShellManager
from framework_shells.native_pipe import resolve_native_terminal_broker_command
from framework_shells.orchestrator import Orchestrator
from framework_shells.store import RuntimeStore

resolution = resolve_native_terminal_broker_command(["fallback-broker"])
assert resolution.engine is None, resolution
assert resolution.command == ["fallback-broker"], resolution

SPEC_TEXT = '''version: "1"
shells:
  terminal:
    backend: pipe
    pipe:
      mode: native_terminal_pipe_testing
    cwd: ${ctx:CWD}
    env:
      TERMINAL_STREAM_CWD: ${ctx:CWD}
      TERMINAL_STREAM_COLS: ${ctx:COLS}
      TERMINAL_STREAM_ROWS: ${ctx:ROWS}
      TERMINAL_STREAM_SHELL_CMD_JSON: ${ctx:SHELL_CMD_JSON}
'''

async def main() -> None:
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        spec_path = tmp_path / "terminal.yaml"
        spec_path.write_text(SPEC_TEXT, encoding="utf-8")
        store = RuntimeStore(tmp_path / "runtime")
        mgr = FrameworkShellManager(store=store)
        orch = Orchestrator(mgr)
        try:
            await orch.start_from_ref(
                "terminal.yaml#terminal",
                base_dir=tmp_path,
                ctx={
                    "CWD": str(tmp_path),
                    "COLS": "80",
                    "ROWS": "24",
                    "SHELL_CMD_JSON": json.dumps(["sh", "-lc", "printf hello; exit 0"]),
                },
                label="wheel-smoke-terminal",
                wait_ready=False,
            )
        except RuntimeError as exc:
            message = str(exc)
            assert "native terminal broker unavailable" in message, message
        else:
            raise AssertionError("native-only terminal shell unexpectedly launched without a bundled broker")

asyncio.run(main())
"""


def smoke_test_wheel(wheel_path: Path, *, expect_native_broker: bool) -> None:
    if not wheel_path.is_file():
        raise FileNotFoundError(f"Wheel not found: {wheel_path}")

    with tempfile.TemporaryDirectory(prefix="fws-wheel-smoke-") as tmp:
        tmp_path = Path(tmp)
        venv_dir = tmp_path / "venv"
        venv.EnvBuilder(with_pip=True, system_site_packages=True, clear=True).create(venv_dir)
        python_bin = venv_python_path(venv_dir)

        run(
            [
                str(python_bin),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-deps",
                "--force-reinstall",
                str(wheel_path),
            ],
            cwd=tmp_path,
        )

        script = _NATIVE_SMOKE if expect_native_broker else _PURE_SMOKE
        run(
            [str(python_bin), "-c", textwrap.dedent(script)],
            cwd=tmp_path,
            env={
                "PYTHONPATH": "",
                "FRAMEWORK_SHELLS_NATIVE_TERMINAL_BROKER": "",
                "PATH": os.pathsep.join(
                    part
                    for part in ("/system/bin", "/bin", "/usr/bin")
                    if Path(part).exists()
                ),
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an installed-wheel smoke test")
    parser.add_argument("wheel", help="Path to the wheel to test")
    parser.add_argument(
        "--expect-native-broker",
        action="store_true",
        help="Expect the wheel to contain the packaged native terminal broker",
    )
    args = parser.parse_args()
    smoke_test_wheel(
        Path(args.wheel).resolve(),
        expect_native_broker=args.expect_native_broker,
    )


if __name__ == "__main__":
    main()
