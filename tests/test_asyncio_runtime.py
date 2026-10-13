from __future__ import annotations

import asyncio
from argparse import Namespace
import importlib.util
import os
from pathlib import Path
import socket
import sys
import threading
import tempfile
import unittest
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Protocol, cast
from unittest.mock import patch

from framework_shells import asyncio_runtime as runtime
from framework_shells.ferrous_framework import FerrousFrameworkHost, FerrousFrameworkShell
from framework_shells.native_pipe import native_extension_available

Runner = Callable[[Coroutine[object, object, int]], int]


class _Closable(Protocol):
    def close(self) -> None: ...


class AsyncioRuntimeTests(unittest.TestCase):
    def test_missing_module_uses_asyncio(self) -> None:
        with patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "0"}), patch.object(
            runtime.importlib, "import_module",
            side_effect=ModuleNotFoundError("missing", name="uvloop"),
        ):
            loop = runtime.new_event_loop()
            try:
                self.assertFalse(type(loop).__module__.startswith("uvloop"))
            finally:
                loop.close()

    def test_disabled_does_not_import(self) -> None:
        for flag in ("1", "true", "YES", " on "):
            with self.subTest(flag=flag), patch.dict(os.environ, {
                "FRAMEWORK_SHELLS_DISABLE_UVLOOP": flag,
            }), patch.object(runtime.importlib, "import_module") as importer:
                loop = runtime.new_event_loop()
                loop.close()
                importer.assert_not_called()

    def test_broken_installs_fail_visibly(self) -> None:
        for error in (ImportError("ABI mismatch"), ModuleNotFoundError("nested", name="dependency")):
            with self.subTest(error=error), patch.dict(os.environ, {
                "FRAMEWORK_SHELLS_DISABLE_UVLOOP": "0",
            }), patch.object(runtime.importlib, "import_module", side_effect=error):
                with self.assertRaises(type(error)):
                    _ = runtime.new_event_loop()

    def test_background_factory_owns_and_closes_loop(self) -> None:
        owners: list[int] = []
        loops: list[asyncio.AbstractEventLoop] = []

        class Module:
            def new_event_loop(self) -> asyncio.AbstractEventLoop:
                owners.append(threading.get_ident())
                loop = asyncio.new_event_loop()
                loops.append(loop)
                return loop

        async def work() -> int:
            await asyncio.sleep(0)
            return 42

        results: list[int] = []
        with patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "0"}), patch.object(
            runtime.importlib, "import_module", return_value=Module(),
        ) as importer:
            thread = threading.Thread(target=lambda: results.append(runtime.run(work())))
            thread.start()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            importer.assert_called_once_with("uvloop")
        self.assertEqual(results, [42])
        self.assertEqual(owners, [thread.ident])
        self.assertTrue(loops[0].is_closed())

    def runners(self) -> tuple[Runner, Runner]:
        return runtime.run, cast(Runner, getattr(runtime, "_run_legacy"))

    def test_runner_cleanup_modern_and_legacy(self) -> None:
        for run in self.runners():
            cleaned: list[str] = []

            async def pending() -> None:
                try:
                    await asyncio.sleep(60)
                finally:
                    cleaned.append("task")

            async def values() -> AsyncIterator[int]:
                try:
                    yield 1
                finally:
                    cleaned.append("generator")

            held: list[AsyncIterator[int]] = []

            async def work() -> int:
                _ = asyncio.create_task(pending())
                generator = values()
                held.append(generator)
                _ = await generator.__anext__()
                return await asyncio.to_thread(lambda: 7)

            with self.subTest(run=run), patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "1"}):
                self.assertEqual(run(work()), 7)
                self.assertCountEqual(cleaned, ["task", "generator"])

    def test_factory_failure_closes_coroutine(self) -> None:
        async def work() -> int:
            return 1

        for run in self.runners():
            coroutine = work()
            with patch.object(runtime, "new_event_loop", side_effect=ImportError("bad ABI")):
                with self.assertRaises(ImportError):
                    _ = run(coroutine)
            self.assertIsNone(coroutine.cr_frame)

    def test_main_failure_still_cancels_pending_tasks(self) -> None:
        for run in self.runners():
            cleaned: list[bool] = []

            async def pending() -> None:
                try:
                    await asyncio.sleep(60)
                finally:
                    await asyncio.sleep(0)
                    cleaned.append(True)

            async def work() -> int:
                _ = asyncio.create_task(pending())
                await asyncio.sleep(0)
                raise ValueError("main failure")

            with patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "1"}):
                with self.assertRaisesRegex(ValueError, "main failure"):
                    _ = run(work())
            self.assertEqual(cleaned, [True])

    def test_running_caller_is_not_replaced(self) -> None:
        async def value() -> int:
            return 1

        async def caller() -> None:
            original = asyncio.get_running_loop()
            with patch.object(runtime, "new_event_loop") as factory:
                coroutine = value()
                with self.assertRaisesRegex(RuntimeError, "running event loop"):
                    _ = runtime.run(coroutine)
                factory.assert_not_called()
                self.assertIsNone(coroutine.cr_frame)
                self.assertIs(asyncio.get_running_loop(), original)
        asyncio.run(caller())

    def test_bridge_factory_failure_reports_readiness(self) -> None:
        # No child or server starts: failure is injected at loop creation.
        with patch.dict(os.environ, {}), patch.object(runtime, "new_event_loop", side_effect=ImportError("bad ABI")), patch(
            "framework_shells.ferrous_framework._framework_secret", return_value="isolated-loop-test",
        ):
            with self.assertRaisesRegex(RuntimeError, "failed to start") as host_error:
                _ = FerrousFrameworkHost(port=0)
            self.assertIsInstance(host_error.exception.__cause__, ImportError)
            with self.assertRaisesRegex(RuntimeError, "failed to start") as shell_error:
                _ = FerrousFrameworkShell(["unused"], None, {}, "test", "test", [])
            self.assertIsInstance(shell_error.exception.__cause__, ImportError)

    def test_cli_runs_command_on_owned_loop(self) -> None:
        from framework_shells.cli.main import main

        commands: list[str] = []

        async def command(args: Namespace) -> None:
            self.assertFalse(type(asyncio.get_running_loop()).__module__.startswith("uvloop"))
            commands.append(str(args.command))

        with patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "1"}), patch(
            "sys.argv", ["fws", "list"],
        ), patch("framework_shells.cli.main.setup_environment"), patch(
            "framework_shells.cli.main.run_async", side_effect=command,
        ):
            main()
        self.assertEqual(commands, ["list"])

    @unittest.skipUnless(importlib.util.find_spec("uvloop"), "uvloop not installed")
    def test_real_uvloop_subprocess_fd_and_cleanup(self) -> None:
        async def work() -> int:
            loop = asyncio.get_running_loop()
            self.assertTrue(type(loop).__module__.startswith("uvloop"))
            reader, writer = socket.socketpair()
            reader.setblocking(False)
            received: asyncio.Future[bytes] = loop.create_future()

            def readable() -> None:
                loop.remove_reader(reader.fileno())
                received.set_result(reader.recv(128))

            try:
                loop.add_reader(reader.fileno(), readable)
                writer.sendall(b"opaque\x00\xff")
                self.assertEqual(await asyncio.wait_for(received, 5), b"opaque\x00\xff")
            finally:
                loop.remove_reader(reader.fileno())
                reader.close()
                writer.close()
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-I", "-c",
                "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            )
            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(b"binary\x00\xff"), 5)
                self.assertEqual(stdout, b"binary\x00\xff")
                self.assertEqual(proc.returncode, 0)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    _ = await proc.wait()
            return 1

        with patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_UVLOOP": "0"}):
            for run in self.runners():
                self.assertEqual(run(work()), 1)

    @unittest.skipUnless(importlib.util.find_spec("uvloop") and native_extension_available(),
                         "uvloop and compatible native pipe extension required")
    def test_native_pipe_on_both_selected_loops(self) -> None:
        from framework_shells.manager import FrameworkShellManager
        from framework_shells.store import RuntimeStore

        for disabled in ("0", "1"):
            with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "FRAMEWORK_SHELLS_DISABLE_UVLOOP": disabled,
                "FRAMEWORK_SHELLS_SECRET": "isolated-uvloop-native-test",
                "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1",
            }):
                async def work() -> int:
                    self.assertEqual(type(asyncio.get_running_loop()).__module__.startswith("uvloop"),
                                     disabled == "0")
                    manager = FrameworkShellManager(store=RuntimeStore(base_dir=Path(directory)),
                                                    enable_procfs_process_discovery=False)
                    record = await manager.spawn_shell_pipe([
                        sys.executable, "-I", "-c",
                        "import os\nwhile data := os.read(0, 65536): os.write(1, data)",
                    ], pipe_config={"mode": "native_pipe_testing"})
                    state = manager.get_pipe_state(record.id)
                    assert state is not None and state.process is not None
                    process = state.process
                    subscription = await manager.subscribe_output_bytes(record.id)
                    try:
                        self.assertEqual(state.native_engine, "native-pipe")
                        self.assertIsNotNone(state.native_pump)
                        await manager.write_to_pipe(record.id, "native-ready\x00\n")
                        output = bytearray()
                        while len(output) < len(b"native-ready\x00\n"):
                            output.extend(await asyncio.wait_for(subscription.get(), 5))
                        self.assertEqual(bytes(output), b"native-ready\x00\n")
                    finally:
                        await manager.unsubscribe_output_bytes(record.id, subscription)
                        try:
                            await manager.send_pipe_eof(record.id)
                            # Native mode owns a duplicate; asyncio's paused original
                            # otherwise prevents its subprocess waiter observing EOF.
                            if process.stdout is not None:
                                cast(_Closable, getattr(process.stdout, "_transport")).close()
                            _ = await asyncio.wait_for(process.wait(), 5)
                            if state.waiter is not None:
                                await asyncio.wait_for(asyncio.shield(state.waiter), 5)
                        finally:
                            if process.returncode is None:
                                await manager.terminate_shell(record.id, force=True)
                            # Fixture owns the paused transport's original fd too.
                            transport = cast(_Closable, getattr(process, "_transport"))
                            transport.close()
                    self.assertEqual(Path(record.stdout_log).read_bytes(), b"native-ready\x00\n")
                    return 1

                self.assertEqual(runtime.run(work()), 1)


if __name__ == "__main__":
    _ = unittest.main()
