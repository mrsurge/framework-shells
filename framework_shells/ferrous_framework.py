from __future__ import annotations

import asyncio
import os
import queue
import threading
import time
import importlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast, runtime_checkable


class _ShellRecord(Protocol):
    id: object


class _PipeProcess(Protocol):
    stdin: object | None
    returncode: int | None


class _PipeState(Protocol):
    process: _PipeProcess | None


class _ByteSubscription(Protocol):
    async def get(self) -> bytes | str | None: ...


class _FrameworkShellManager(Protocol):
    async def spawn_shell_pipe(
        self,
        command: Sequence[str],
        *,
        cwd: str | None,
        env: Mapping[str, str],
        label: str,
        spec_id: str,
        subgroups: Sequence[str],
        pipe_config: Mapping[str, object],
        autostart: bool,
    ) -> _ShellRecord: ...

    async def subscribe_output_bytes(self, shell_id: str) -> _ByteSubscription: ...

    def get_pipe_state(self, shell_id: str) -> _PipeState | None: ...

    async def write_to_pipe(self, shell_id: str, data: str) -> None: ...

    async def terminate_shell(self, shell_id: str, *, force: bool) -> None: ...

    async def unsubscribe_output_bytes(self, shell_id: str, subscription: _ByteSubscription) -> None: ...


@runtime_checkable
class _FrameworkShellWriter(Protocol):
    async def write_to_shell(self, shell_id: str, data: str, *, append_newline: bool) -> None: ...


class _FrameworkShellsModule(Protocol):
    async def get_manager(self, *, run_id: str) -> _FrameworkShellManager: ...


class _RenderedShellSpec(Protocol):
    env: Mapping[str, str]
    cwd: str | None
    id: str | None
    subgroups: Sequence[str] | None
    pipe: Mapping[str, object] | None

    def normalized_command(self) -> list[str]: ...


class _ShellspecModule(Protocol):
    def load_shellspec(self, path: Path) -> Mapping[str, object]: ...

    def render_shellspec(
        self,
        spec: object,
        *,
        ctx: Mapping[str, str],
        env: Mapping[str, str],
    ) -> _RenderedShellSpec: ...


class FerrousFrameworkPipe:
    def __init__(
        self,
        command: Sequence[str],
        cwd: str | None,
        env: dict[str, str],
        label: str,
        spec_id: str,
        subgroups: Sequence[str],
        shellspec_path: str | None = None,
        shellspec_entry: str | None = None,
    ) -> None:
        self._command = list(command)
        self._cwd = cwd
        self._env = dict(env)
        self._label = label
        self._spec_id = spec_id
        self._subgroups = list(subgroups)
        self._shellspec_path = shellspec_path
        self._shellspec_entry = shellspec_entry
        self._lines: "queue.Queue[str | None]" = queue.Queue()
        self._ready = threading.Event()
        self._ready_error: BaseException | None = None
        self._closed = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._mgr: _FrameworkShellManager | None = None
        self._subscription: _ByteSubscription | None = None
        self._shell_id = ""
        self._thread = threading.Thread(
            target=self._thread_main,
            name="ferrous-framework-pipe",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=10.0):
            raise TimeoutError("timed out starting ferrous_framework pipe")
        if self._ready_error is not None:
            raise RuntimeError("failed to start ferrous_framework pipe") from self._ready_error

    def shell_id(self) -> str:
        return self._shell_id

    def write_line(self, line: str) -> None:
        loop = self._require_loop()
        future = asyncio.run_coroutine_threadsafe(self._write_line(line), loop)
        future.result(timeout=10.0)

    def read_line(self, timeout: float | None = None) -> str | None:
        try:
            return self._lines.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        loop = self._loop
        if loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._terminate(), loop)
        try:
            future.result(timeout=5.0)
        except Exception as exc:
            raise RuntimeError("failed to terminate ferrous_framework pipe") from exc

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run())
        except BaseException as exc:
            self._ready_error = exc
            self._ready.set()
            self._lines.put(None)
        finally:
            self._lines.put(None)
            loop.close()

    async def _run(self) -> None:
        framework_shells = cast(
            _FrameworkShellsModule,
            importlib.import_module("framework_shells"),
        )

        os.environ.update(
            {
                key: value
                for key, value in self._env.items()
                if key.startswith("FRAMEWORK_SHELLS_")
            }
        )
        mgr = await framework_shells.get_manager(
            run_id=os.environ.get("FRAMEWORK_SHELLS_RUN_ID", "app-server")
        )
        self._mgr = mgr
        command, cwd, env, spec_id, subgroups, pipe_config = self._render_shellspec()
        record = await mgr.spawn_shell_pipe(
            command,
            cwd=cwd,
            env=env,
            label=self._label,
            spec_id=spec_id,
            subgroups=subgroups,
            pipe_config=pipe_config,
            autostart=True,
        )
        self._shell_id = str(record.id)
        await self._wait_for_pipe_state()
        self._subscription = await mgr.subscribe_output_bytes(self._shell_id)
        self._ready.set()
        await self._pump_output()

    async def _wait_for_pipe_state(self) -> None:
        deadline = time.monotonic() + 5.0
        while True:
            mgr = self._require_manager()
            state = mgr.get_pipe_state(self._shell_id)
            process = state.process if state is not None else None
            if process is not None and process.stdin is not None:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(f"native pipe stdin never became ready for {self._shell_id}")
            await asyncio.sleep(0.05)

    def _render_shellspec(
        self,
    ) -> tuple[list[str], str | None, dict[str, str], str, list[str], dict[str, object]]:
        if not self._shellspec_path:
            return (
                self._command,
                self._cwd,
                self._env,
                self._spec_id,
                self._subgroups,
                {"mode": "native_pipe_testing"},
            )
        shellspec = cast(
            _ShellspecModule,
            importlib.import_module("framework_shells.shellspec"),
        )

        specs = shellspec.load_shellspec(Path(self._shellspec_path))
        entry = self._shellspec_entry or self._spec_id
        spec = specs.get(entry) if entry else None
        if spec is None and self._shellspec_entry:
            raise KeyError(f"shellspec entry not found: {self._shellspec_entry}")
        if spec is None:
            spec = next(iter(specs.values()))
        rendered = shellspec.render_shellspec(
            spec,
            ctx={
                "PYTHON": self._command[0],
                "CWD": self._cwd or os.getcwd(),
            },
            env=self._env,
        )
        command = rendered.normalized_command()
        env = {**self._env, **dict(rendered.env)}
        return (
            command,
            rendered.cwd or self._cwd,
            env,
            rendered.id or self._spec_id,
            list(rendered.subgroups or self._subgroups),
            dict(rendered.pipe or {"mode": "native_pipe_testing"}),
        )

    async def _pump_output(self) -> None:
        buffer = bytearray()
        try:
            while not self._closed.is_set():
                subscription = self._require_subscription()
                chunk = await subscription.get()
                if not chunk:
                    if self._process_exited():
                        return
                    continue
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8", errors="replace")
                buffer.extend(bytes(chunk))
                while True:
                    newline = buffer.find(b"\n")
                    if newline == -1:
                        break
                    raw = bytes(buffer[:newline])
                    del buffer[: newline + 1]
                    self._lines.put(raw.decode("utf-8", errors="replace"))
        finally:
            if buffer:
                self._lines.put(bytes(buffer).decode("utf-8", errors="replace"))
            self._lines.put(None)
            await self._unsubscribe()

    def _process_exited(self) -> bool:
        state = self._require_manager().get_pipe_state(self._shell_id)
        process = state.process if state is not None else None
        return process is None or process.returncode is not None

    async def _write_line(self, line: str) -> None:
        mgr = self._require_manager()
        if isinstance(mgr, _FrameworkShellWriter):
            await mgr.write_to_shell(self._shell_id, line, append_newline=True)
            return
        await mgr.write_to_pipe(self._shell_id, f"{line}\n")

    async def _terminate(self) -> None:
        if self._mgr is not None and self._shell_id:
            await self._mgr.terminate_shell(self._shell_id, force=True)
        await self._unsubscribe()

    async def _unsubscribe(self) -> None:
        if self._mgr is None or self._subscription is None:
            return
        subscription = self._subscription
        self._subscription = None
        await self._mgr.unsubscribe_output_bytes(self._shell_id, subscription)

    def _require_manager(self) -> _FrameworkShellManager:
        if self._mgr is None:
            raise RuntimeError("ferrous_framework manager is not ready")
        return self._mgr

    def _require_subscription(self) -> _ByteSubscription:
        if self._subscription is None:
            raise RuntimeError("ferrous_framework output subscription is not ready")
        return self._subscription

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("ferrous_framework pipe loop is not running")
        return self._loop
