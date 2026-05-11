from __future__ import annotations

import asyncio
import fcntl
import json
import os
import pty
import re
import select
import shlex
import shutil
import signal
import struct
import subprocess
import termios
import time
import uuid
import inspect
from asyncio import Lock as AsyncLock
from asyncio import Queue as AsyncQueue
from collections.abc import Mapping
from pathlib import Path
from typing import AsyncIterator, Dict, Iterable, List, Optional, Protocol, cast

import aiofiles

from .store import RuntimeStore
from .record import (
    BACKEND_DTACH,
    BACKEND_PIPE,
    BACKEND_PROC,
    BACKEND_PTY,
    ShellRecord,
    normalize_backend,
)
from .pty import PTYState, PTYWriteRequest, PipeState
from .events import EventBus, EventType, ShellEvent, get_event_bus
from .hooks import ShellLifecycleHooks
from .process_snapshot import (
    ExternalProcessProvider,
    ProcfsProcessProvider,
    ProcessRecord,
    ProcessSnapshot,
    collect_external_processes,
)
from .log_inspection import (
    JSON_FORMAT,
    JSONRPC_FORMAT,
    PLAIN_FORMAT,
    inspect_log_file,
    read_event_window,
)
from .native_pipe import (
    NATIVE_PIPE_TESTING_MODE,
    NATIVE_TERMINAL_BROKER_BIN,
    NATIVE_TERMINAL_PIPE_ENGINE,
    NATIVE_TERMINAL_PIPE_TESTING_MODE,
    PYTHON_TERMINAL_PIPE_ENGINE,
    PYTHON_TERMINAL_PIPE_TESTING_MODE,
    NativePipePumpHandle,
    create_native_pipe_pump,
    is_native_terminal_placeholder_command,
    native_extension_phase,
    native_extension_available,
    normalize_pipe_config,
    resolve_native_terminal_broker_command,
    resolve_python_terminal_broker_command,
    resolve_terminal_broker_fallback_command,
)
from .shutdown import ShutdownPolicy, shutdown_snapshot

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

HOME_DIR = Path(os.path.expanduser("~"))
PTY_MODE_RAW = "raw"
PTY_MODE_INTERACTIVE = "interactive"
JSONMap = dict[str, object]
JSONList = list[JSONMap]


class _PipeReadTransport(Protocol):
    def get_extra_info(self, name: str, default: object | None = None) -> object:
        ...

    def pause_reading(self) -> None:
        ...

    def resume_reading(self) -> None:
        ...


class _HasFileno(Protocol):
    def fileno(self) -> int:
        ...

def _truthy_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    return val in {"1", "true", "yes", "y", "on"}

def _shell_debug(stage: str, message: str) -> None:
    # TODO: proper logging
    print(f"[PTY][{stage}] {message}")


def _normalize_pty_mode(value: Optional[str], *, default: str = PTY_MODE_RAW) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    if raw in {PTY_MODE_RAW, PTY_MODE_INTERACTIVE}:
        return raw
    raise ValueError(f"Invalid pty_mode: {value!r} (expected 'raw' or 'interactive')")

class FrameworkShellManager:
    """Creates and tracks background framework shells with runtime isolation."""

    store: RuntimeStore
    metadata_dir: Path
    logs_dir: Path
    sockets_dir: Path
    max_app_shells: int
    max_service_shells: int
    run_id: str | None
    launcher_pid: int
    started_at: float
    _pty: dict[str, PTYState]
    _pipes: dict[str, PipeState]
    _event_bus: EventBus
    _lock_instance: AsyncLock | None
    _dtach_bin: str | None
    _enable_dtach_proxy: bool
    _signal_winch_on_resize: bool
    _default_pty_mode: str
    _hooks: ShellLifecycleHooks | None
    external_process_provider: ExternalProcessProvider | None
    _procfs_provider: ProcfsProcessProvider | None

    def __init__(
        self,
        *,
        store: Optional[RuntimeStore] = None,
        max_app_shells: Optional[int] = None,
        max_service_shells: Optional[int] = None,
        run_id: Optional[str] = None,
        enable_dtach_proxy: bool = True,
        signal_winch_on_resize: Optional[bool] = None,
        default_pty_mode: Optional[str] = None,
        process_hooks: Optional[ShellLifecycleHooks] = None,
        external_process_provider: Optional[ExternalProcessProvider] = None,
        enable_procfs_process_discovery: bool = True,
    ) -> None:
        self.store = store or RuntimeStore()
        self.metadata_dir = self.store.metadata_dir
        self.logs_dir = self.store.logs_dir
        self.sockets_dir = self.store.sockets_dir
        
        self.max_app_shells = max_app_shells if max_app_shells is not None else 5
        self.max_service_shells = max_service_shells if max_service_shells is not None else 5
        self.run_id = run_id
        self.launcher_pid = os.getpid()
        self.started_at = time.time()
        self._pty = {}
        self._pipes = {}
        
        self._event_bus = get_event_bus()
        self._lock_instance = None
        self._dtach_bin = shutil.which("dtach")
        self._enable_dtach_proxy = bool(enable_dtach_proxy)
        self._signal_winch_on_resize = (
            _truthy_env("FRAMEWORK_SHELLS_SIGWINCH_ON_RESIZE", default=False)
            if signal_winch_on_resize is None
            else bool(signal_winch_on_resize)
        )
        self._default_pty_mode = _normalize_pty_mode(
            default_pty_mode if default_pty_mode is not None else os.environ.get("FRAMEWORK_SHELLS_PTY_MODE"),
            default=PTY_MODE_RAW,
        )
        self._hooks = process_hooks
        self.external_process_provider = external_process_provider
        self._procfs_provider = ProcfsProcessProvider() if enable_procfs_process_discovery else None
        try:
            from .fws_socketio_peer import ensure_fws_socketio_peer_started

            asyncio.get_running_loop().create_task(ensure_fws_socketio_peer_started())
        except Exception:
            pass

    async def build_process_snapshot(
        self,
        *,
        shells: Optional[List[ShellRecord]] = None,
        include_procfs_descendants: bool = True,
    ) -> ProcessSnapshot:
        """Build a best-effort process snapshot for UI and shutdown planning.

        The snapshot is intentionally host-agnostic. If a host provides an
        `external_process_provider`, those processes are merged in. A procfs
        provider may also be used to discover descendants of managed shells so
        standalone usage still has a tree.

        Merge priority (highest wins on PID collisions):
          1) managed shells
          2) host-provided external process provider
          3) procfs-derived process discovery
        """
        shells = shells or await self.list_shells()
        root_pids = [rec.pid for rec in shells if rec.pid and rec.status == "running"]

        procfs: List[ProcessRecord] = []
        if include_procfs_descendants and self._procfs_provider:
            try:
                procfs = await asyncio.to_thread(self._procfs_provider.list_processes, root_pids=root_pids)
            except Exception:
                procfs = []

        external: List[ProcessRecord] = []
        if self.external_process_provider:
            try:
                external = await collect_external_processes(self.external_process_provider, root_pids=root_pids)
            except Exception:
                external = []

        processes: Dict[int, ProcessRecord] = {}
        for rec in procfs:
            processes[rec.pid] = rec
        for rec in external:
            processes[rec.pid] = rec

        for shell in shells:
            if not shell.pid:
                continue
            processes[shell.pid] = ProcessRecord(
                pid=shell.pid,
                parent_pid=shell.launcher_pid,
                type="shell",
                label=shell.label or shell.id,
                metadata={
                    "shell_id": shell.id,
                    "run_id": shell.run_id,
                    "backend": self._backend_name(shell),
                    "uses_dtach": bool(getattr(shell, "uses_dtach", False)),
                    "uses_pipes": bool(getattr(shell, "uses_pipes", False)),
                    "uses_pty": bool(getattr(shell, "uses_pty", False)),
                },
                shell_id=shell.id,
            )

        return ProcessSnapshot(captured_at=time.time(), processes=processes)

    async def shutdown_app_group(self, app_id: str) -> dict[str, object]:
        """UI-equivalent shutdown for an app/group id.

        Mirrors `/fws/action/app/{app_id}/shutdown` behavior: find running shells
        with matching `derive_app_id()`, snapshot descendants, then shutdown that
        subtree via `root_pids`.
        """
        app_id = str(app_id or "").strip()
        if not app_id:
            return {"ok": True, "data": {"root_pids": [], "stats": {}, "note": "empty app_id"}}

        shells = await self.list_shells()
        targets = [
            s
            for s in shells
            if (s.derive_app_id() or "") == app_id and s.pid and s.status == "running"
        ]
        root_pids = [int(s.pid) for s in targets if s.pid]
        if not root_pids:
            return {"ok": True, "data": {"root_pids": [], "stats": {}, "note": "no matching running shells"}}

        snapshot = await self.build_process_snapshot(shells=shells, include_procfs_descendants=True)
        stats = await shutdown_snapshot(
            snapshot,
            manager=self,
            policy=ShutdownPolicy(types_last=[]),
            root_pids=root_pids,
        )
        return {"ok": True, "data": {"root_pids": root_pids, "stats": stats}}

    def _fire_hook(self, result: object) -> None:
        """Best-effort hook execution; never blocks core flow."""
        if result is None:
            return
        if not inspect.isawaitable(result):
            return
        try:
            asyncio.ensure_future(result)
        except Exception:
            return

    def _run_hook_running(self, record: ShellRecord) -> None:
        hook = self._hooks.on_shell_running if self._hooks else None
        if not hook:
            return
        try:
            self._fire_hook(hook(record))
        except Exception:
            return

    def _run_hook_adopted(self, record: ShellRecord) -> None:
        hook = self._hooks.on_shell_adopted if self._hooks else None
        if not hook:
            return
        try:
            self._fire_hook(hook(record))
        except Exception:
            return

    def _run_hook_exited(self, record: ShellRecord, last_pid: Optional[int]) -> None:
        hook = self._hooks.on_shell_exited if self._hooks else None
        if not hook:
            return
        try:
            self._fire_hook(hook(record, last_pid))
        except Exception:
            return

    def _get_lock(self) -> AsyncLock:
        if self._lock_instance is None:
            self._lock_instance = AsyncLock()
        return self._lock_instance

    async def _emit(self, event_type: EventType, record: ShellRecord, **extra: object) -> None:
        event = ShellEvent(
            type=event_type,
            shell_id=record.id,
            data={**record.to_payload(), **extra},
            app_id=record.app_id or record.derive_app_id(),
            parent_shell_id=record.parent_shell_id,
            is_app_worker=record.is_app_worker,
        )
        await self._event_bus.publish(event)

    async def emit_log_reset(self, shell_id: str, stream_name: str) -> None:
        record = await self._load_record(shell_id)
        event = ShellEvent(
            type=EventType.LOG_RESET,
            shell_id=shell_id,
            data={"stream": stream_name},
            app_id=(record.app_id or record.derive_app_id()) if record is not None else None,
            parent_shell_id=record.parent_shell_id if record is not None else None,
            is_app_worker=record.is_app_worker if record is not None else False,
        )
        await self._event_bus.publish(event)

    # ------------------------------------------------------------------
    # Adoption and helpers

    async def _adopt_orphaned_shells(self) -> None:
        """Adopt shells from previous runs. Caller must hold the lock."""
        stale_records: List[ShellRecord] = []
        updated = 0
        async for record in self._aiter_records():
            alive = bool(record.pid) and await self._is_pid_alive(record.pid)
            
            # Special check for dtach: process might be alive even if we aren't attached
            # But record.pid tracks the actual shell inside dtach.
            
            if record.pid and not alive:
                exit_code = record.exit_code or await self._collect_exit_code(record.pid)
                await self._mark_exited(record, exit_code)
                record.pid = None
                record.status = "exited"
                stale_records.append(record)
                continue
            if not alive and not (record.uses_dtach and record.status == "running"):
                # If dead and not dtach (or dtach assumed dead), clean up
                stale_records.append(record)
                continue
            
            if not self.run_id:
                continue

            mutated = False
            if not record.run_id or record.run_id != self.run_id:
                record.run_id = self.run_id
                mutated = True
            if record.launcher_pid != self.launcher_pid:
                record.launcher_pid = self.launcher_pid
                mutated = True
            
            if self._enable_dtach_proxy and record.uses_dtach and record.id not in self._pty and alive:
                # Re-attach logic for dtach
                try:
                    await self._attach_dtach_proxy(record)
                    mutated = True  # Considered adoption action
                except Exception:
                    pass

            if mutated:
                record.adopted = True
                await self._save_record(record)
                if alive and record.status == "running":
                    self._run_hook_adopted(record)
                updated += 1
        
        for record in stale_records:
            await self._stop_pty(record.id)
            await self._stop_pipe(record.id)
            # Cleanup omitted for safety

        if updated:
            print(f"[FrameworkShells] Adopted {updated} running shell(s) from previous run")

    async def adopt_orphaned_shells(self) -> None:
        async with self._get_lock():
            await self._adopt_orphaned_shells()

    async def list_active_pids(self) -> List[int]:
        async with self._get_lock():
            pids: List[int] = []
            async for record in self._aiter_records():
                if record.pid and await self._is_pid_alive(record.pid):
                    pids.append(record.pid)
            return pids

    async def aggregate_resource_stats(self) -> dict[str, object]:
        async with self._get_lock():
            now = time.time()
            num_shells = 0
            num_running = 0
            pids: list[int] = []
            stats: dict[str, object] = {
                "run_id": self.run_id,
                "launcher_pid": self.launcher_pid,
                "started_at": self.started_at,
                "uptime": max(0.0, now - self.started_at),
                "num_shells": num_shells,
                "num_running": num_running,
                "num_adopted": 0,
                "cpu_percent": 0.0,
                "memory_rss": 0,
                "pids": pids,
                "has_psutil": bool(psutil),
            }
            running_records: List[ShellRecord] = []
            adopted_count = 0
            async for record in self._aiter_records():
                num_shells += 1
                if getattr(record, "adopted", False):
                    adopted_count += 1
                if record.pid and await self._is_pid_alive(record.pid):
                    num_running += 1
                    pids.append(record.pid)
                    running_records.append(record)
            stats["num_shells"] = num_shells
            stats["num_running"] = num_running
            stats["num_adopted"] = adopted_count
            if psutil:
                cpu_total = 0.0
                rss_total = 0
                for rec in running_records:
                    try:
                        proc = await asyncio.to_thread(psutil.Process, rec.pid)  # type: ignore[arg-type]
                        with proc.oneshot():
                            cpu_total += proc.cpu_percent(interval=0.0)
                            rss_total += proc.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                stats["cpu_percent"] = cpu_total
                stats["memory_rss"] = rss_total
            return stats

    # ------------------------------------------------------------------
    # Persistence

    async def _aiter_records(self) -> AsyncIterator[ShellRecord]:
        meta_paths = sorted(self.metadata_dir.glob("*/meta.json"))
        for meta in meta_paths:
            record = await self._load_record(meta.parent.name)
            if record:
                yield record

    async def _load_record(self, shell_id: str) -> Optional[ShellRecord]:
        meta_path = self.metadata_dir / shell_id / "meta.json"
        if not meta_path.exists():
            return None
        try:
            async with aiofiles.open(meta_path, "r", encoding="utf-8") as fh:
                content = await fh.read()
                raw_data: object = json.loads(content)
                if not isinstance(raw_data, dict):
                    return None
                data = cast(dict[str, object], raw_data)
        except Exception:
            return None

        # Verify signature using on-disk payload (forward-compatible with added fields).
        try:
            from .auth import derive_runtime_id, verify_record

            if data.get("runtime_id") != derive_runtime_id(self.store.secret):
                return None
            if not verify_record(self.store.secret, data):
                return None
        except Exception:
            return None
        
        try:
            def get_list(k: str) -> list[str]:
                value = data.get(k)
                if not isinstance(value, list):
                    return []
                return [str(item) for item in cast(list[object], value)]

            def get_dict(k: str) -> dict[str, object]:
                value = data.get(k)
                return dict(cast(dict[str, object], value)) if isinstance(value, dict) else {}

            def get_str(k: str, default: str | None = None) -> str | None:
                value = data.get(k)
                return value if isinstance(value, str) else default

            def get_int(k: str) -> int | None:
                value = data.get(k)
                return value if isinstance(value, int) and not isinstance(value, bool) else None

            def get_float(k: str, default: float) -> float:
                value = data.get(k)
                if isinstance(value, bool):
                    return default
                if isinstance(value, int | float | str):
                    try:
                        return float(value)
                    except ValueError:
                        return default
                return default

            command_value = data.get("command")
            command = [str(item) for item in cast(list[object], command_value)] if isinstance(command_value, list) else []
            env_overrides = {str(k): str(v) for k, v in get_dict("env_overrides").items()}
            record_id = get_str("id", shell_id) or shell_id
            record = ShellRecord(
                id=record_id,
                command=command,
                label=get_str("label"),
                subgroups=get_list("subgroups"),
                ui=get_dict("ui"),
                cwd=get_str("cwd", str(HOME_DIR)) or str(HOME_DIR),
                env_overrides=env_overrides,
                pid=get_int("pid"),
                status=get_str("status", "unknown") or "unknown",
                created_at=get_float("created_at", time.time()),
                updated_at=get_float("updated_at", time.time()),
                autostart=bool(data.get("autostart", False)),
                stdout_log=get_str("stdout_log", str(self.logs_dir / f"{record_id}.stdout.log")) or str(self.logs_dir / f"{record_id}.stdout.log"),
                stderr_log=get_str("stderr_log", str(self.logs_dir / f"{record_id}.stderr.log")) or str(self.logs_dir / f"{record_id}.stderr.log"),
                spec_id=get_str("spec_id"),
                exit_code=get_int("exit_code"),
                run_id=get_str("run_id"),
                launcher_pid=get_int("launcher_pid"),
                adopted=bool(data.get("adopted", False)),
                backend=normalize_backend(
                    get_str("backend"),
                    uses_pty=bool(data.get("uses_pty", False)),
                    uses_pipes=bool(data.get("uses_pipes", False)),
                    uses_dtach=bool(data.get("uses_dtach", False)),
                ),
                uses_pty=bool(data.get("uses_pty", False)),
                uses_pipes=bool(data.get("uses_pipes", False)),
                uses_dtach=bool(data.get("uses_dtach", False)),
                pty_mode=_normalize_pty_mode(get_str("pty_mode"), default=PTY_MODE_RAW),
                runtime_id=get_str("runtime_id"),
                signature=get_str("signature"),
                app_id=get_str("app_id"),
                parent_shell_id=get_str("parent_shell_id"),
                is_app_worker=bool(data.get("is_app_worker", False)),
            )
            return record
        except Exception:
            return None

    async def _save_record(self, record: ShellRecord) -> None:
        record.sign(self.store.secret)
        record_dir = self.metadata_dir / record.id
        await asyncio.to_thread(record_dir.mkdir, parents=True, exist_ok=True)
        tmp_path = record_dir / "meta.json.tmp"
        meta_path = record_dir / "meta.json"
        
        data = record.to_dict()
        async with aiofiles.open(tmp_path, "w", encoding="utf-8") as fh:
            await fh.write(json.dumps(data, indent=2))
        await asyncio.to_thread(tmp_path.replace, meta_path)

    # ------------------------------------------------------------------
    # Core

    def _normalize_command(self, command: Iterable[str]) -> List[str]:
        if isinstance(command, str): command = shlex.split(command)
        cmd_list = [str(part) for part in command]
        if not cmd_list: raise ValueError("command must contain at least one argument")
        return cmd_list

    def _resolve_cwd(self, cwd: Optional[str]) -> str:
        target = Path(os.path.expanduser(cwd or str(HOME_DIR))).resolve()
        if not target.exists(): target.mkdir(parents=True, exist_ok=True)
        return str(target)

    def _prepare_env(self, record: ShellRecord) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(record.env_overrides)
        return env

    def _configure_pty_slave(self, slave_fd: int, *, pty_mode: str) -> None:
        mode = _normalize_pty_mode(pty_mode, default=self._default_pty_mode)
        if mode != PTY_MODE_RAW:
            return
        try:
            attrs = termios.tcgetattr(slave_fd)
            attrs[0] = attrs[0] & ~(termios.ICRNL | termios.IXON)
            attrs[1] = attrs[1] & ~termios.OPOST
            attrs[3] = attrs[3] & ~(termios.ICANON | termios.ECHO | termios.ISIG)
            attrs[6][termios.VMIN] = 1
            attrs[6][termios.VTIME] = 0
            termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
        except Exception:
            pass

    def _set_fd_nonblocking(self, fd: int) -> None:
        try:
            flags = int(fcntl.fcntl(fd, fcntl.F_GETFL))
            if flags & os.O_NONBLOCK:
                return
            _ = fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        except Exception:
            pass

    def _create_record(
        self,
        command: Iterable[str],
        *,
        cwd: Optional[str],
        env: Optional[Dict[str, str]],
        label: Optional[str],
        spec_id: Optional[str] = None,
        subgroups: Optional[List[str]] = None,
        ui: Optional[dict[str, object]] = None,
        autostart: bool,
        uses_pty: bool = False,
        uses_pipes: bool = False,
        uses_dtach: bool = False,
        backend: Optional[str] = None,
        pty_mode: Optional[str] = None,
        parent_shell_id: Optional[str] = None,
    ) -> ShellRecord:
        shell_id = f"fs_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        command_list = self._normalize_command(command)
        cwd_path = self._resolve_cwd(cwd)
        overrides = dict(env or {})
        normalized_subgroups = [str(v).strip() for v in (subgroups or []) if str(v).strip()]
        resolved_pty_mode = _normalize_pty_mode(pty_mode, default=self._default_pty_mode)
        resolved_backend = normalize_backend(
            backend,
            uses_pty=uses_pty,
            uses_pipes=uses_pipes,
            uses_dtach=uses_dtach,
        )

        record = ShellRecord(
            id=shell_id,
            command=command_list,
            label=label,
            subgroups=normalized_subgroups,
            ui=ui or {},
            cwd=cwd_path,
            env_overrides=overrides,
            pid=None,
            status="pending",
            created_at=time.time(),
            updated_at=time.time(),
            autostart=autostart,
            stdout_log=str(self.logs_dir / f"{shell_id}.stdout.log"),
            stderr_log=str(self.logs_dir / f"{shell_id}.stderr.log"),
            spec_id=spec_id,
            exit_code=None,
            run_id=self.run_id,
            launcher_pid=self.launcher_pid,
            adopted=False,
            backend=resolved_backend,
            uses_pty=uses_pty,
            uses_pipes=uses_pipes,
            uses_dtach=uses_dtach,
            pty_mode=resolved_pty_mode,
            parent_shell_id=parent_shell_id
        )
        record.app_id = record.derive_app_id()
        record.is_app_worker = (record.label or "").startswith("app-worker:")
        return record

    async def _launch(self, record: ShellRecord) -> ShellRecord:
        record.set_backend(BACKEND_PROC)
        env = self._prepare_env(record)

        await self._emit(EventType.SHELL_CREATED, record)

        proc = await asyncio.create_subprocess_exec(
            *record.command,
            cwd=record.cwd,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        record.pid = proc.pid
        record.status = "running"
        record.updated_at = time.time()
        await self._save_record(record)

        await self._emit(EventType.SHELL_SPAWNED, record)
        self._run_hook_running(record)

        state = PipeState(
            process=proc,
            label=record.label,
            shell_id=record.id,
            stdin_supported=False,
        )
        state.stdout_reader = asyncio.create_task(
            self._live_stream_reader(record, state, stream_name="stdout", stream=proc.stdout, log_path=Path(record.stdout_log))
        )
        state.stderr_reader = asyncio.create_task(
            self._live_stream_reader(record, state, stream_name="stderr", stream=proc.stderr, log_path=Path(record.stderr_log))
        )
        state.waiter = asyncio.create_task(self._pipe_waiter(record, state))
        self._pipes[record.id] = state
        return record

    async def _launch_dtach(self, record: ShellRecord) -> ShellRecord:
        if not self._dtach_bin:
             raise RuntimeError("dtach binary not found")

        record.set_backend(BACKEND_DTACH)
        socket_path = self.sockets_dir / f"{record.id}.sock"
        pid_file = self.sockets_dir / f"{record.id}.pid"
        
        # Cleanup stale
        if socket_path.exists():
             socket_path.unlink()
        
        cmd_str = ' '.join(shlex.quote(x) for x in record.command)
        # Use a wrapper to capture PID of the shell inside dtach
        wrapper_cmd = f"echo $$ > {shlex.quote(str(pid_file))}; exec {cmd_str}"
        
        dtach_cmd = [
            self._dtach_bin,
            "-n", str(socket_path),
            "sh", "-c", wrapper_cmd
        ]
        
        # Launch dtach daemon (it exits immediately)
        env = self._prepare_env(record)
        
        await self._emit(EventType.SHELL_CREATED, record)

        # We run this sync/async but dtach -n returns instantly
        proc = await asyncio.create_subprocess_exec(
            *dtach_cmd,
            cwd=record.cwd,
            env=env,
            start_new_session=True
        )
        await proc.wait()
        
        # Poll for pidfile
        start_time = time.time()
        found_pid = None
        while time.time() - start_time < 5.0:
            if pid_file.exists():
                try:
                    async with aiofiles.open(pid_file, "r") as f:
                        content = await f.read()
                        if content.strip():
                             found_pid = int(content.strip())
                             break
                except Exception:
                    pass
            await asyncio.sleep(0.1)
        
        if not found_pid:
            raise RuntimeError("Failed to capture PID from dtach session")

        record.pid = found_pid
        record.status = "running"
        record.updated_at = time.time()
        await self._save_record(record)
        await self._emit(EventType.SHELL_SPAWNED, record)
        self._run_hook_running(record)

        # Attach proxy
        await self._attach_dtach_proxy(record)
        return record

    async def _attach_dtach_proxy(self, record: ShellRecord) -> None:
        """Spawn a local dtach -a process to proxy I/O."""
        socket_path = self.sockets_dir / f"{record.id}.sock"
        if not socket_path.exists():
            return # Cannot attach
            
        master_fd, slave_fd = await asyncio.to_thread(pty.openpty)
        self._set_fd_nonblocking(master_fd)
        self._configure_pty_slave(slave_fd, pty_mode=getattr(record, "pty_mode", PTY_MODE_RAW))

        # dtach -a <socket>
        # Note: dtach -a expects a terminal. We give it slave_fd.
        cmd = [self._dtach_bin or "dtach", "-a", str(socket_path)]
        
        env = os.environ.copy()
        env["TERM"] = "xterm-256color"
        
        # IMPORTANT: We need to handle dtach escape key so it doesn't conflict?
        # dtach default detach key is '^\'. 
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=slave_fd,
            stdin=slave_fd,
            stderr=slave_fd,
            cwd=record.cwd,
            env=env,
            start_new_session=True
        )
        
        await asyncio.to_thread(os.close, slave_fd)
        
        state = PTYState(
            master_fd=master_fd,
            label=record.label,
            shell_id=record.id,
            backend=BACKEND_DTACH,
            proxy_pid=proc.pid # Store proxy PID
        )
        
        state.reader = asyncio.create_task(self._pty_reader(record, state))
        state.writer = asyncio.create_task(self._pty_writer(state))
        self._pty[record.id] = state

    async def _launch_pty(self, record: ShellRecord) -> ShellRecord:
        record.set_backend(BACKEND_PTY)
        master_fd, slave_fd = await asyncio.to_thread(pty.openpty)
        self._set_fd_nonblocking(master_fd)
        envp = self._prepare_env(record)
        envp.setdefault("TERM", "xterm-256color")
        self._configure_pty_slave(slave_fd, pty_mode=getattr(record, "pty_mode", PTY_MODE_RAW))

        await self._emit(EventType.SHELL_CREATED, record)

        try:
            proc = await asyncio.create_subprocess_exec(
                *record.command,
                cwd=record.cwd,
                env=envp,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                start_new_session=True,
            )
        finally:
            await asyncio.to_thread(os.close, slave_fd)
        
        record.pid = proc.pid
        record.status = "running"
        record.updated_at = time.time()
        await self._save_record(record)
        
        await self._emit(EventType.SHELL_SPAWNED, record)
        self._run_hook_running(record)
        
        state = PTYState(
            master_fd=master_fd,
            label=record.label,
            shell_id=record.id,
            backend=BACKEND_PTY,
        )
        
        state.reader = asyncio.create_task(self._pty_reader(record, state))
        state.writer = asyncio.create_task(self._pty_writer(state))
        self._pty[record.id] = state
        return record

    async def _pty_reader(self, record: ShellRecord, state: PTYState) -> None:
        log_path = Path(record.stdout_log)
        async with aiofiles.open(log_path, "ab") as log_fh:
            while not state.stop.is_set():
                try:
                    ready = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None, 
                            lambda: select.select([state.master_fd], [], [], 0.5)
                        ),
                        timeout=0.6
                    )
                    rlist, _, _ = ready
                    if not rlist:
                        continue
                    
                    data = await asyncio.to_thread(os.read, state.master_fd, 4096)
                    if not data:
                        break
                    
                    await log_fh.write(data)
                    await log_fh.flush()
                    
                    text = data.decode("utf-8", errors="replace")
    
                    if True:
                        event = ShellEvent(
                           type=EventType.PTY_CHUNK,
                           shell_id=record.id,
                           data={"chunk": text}
                        )
                        await self._event_bus.publish(event)

                    for q in list(state.subscribers):
                        try:
                            await q.put(text)
                        except Exception:
                            pass
                    for q in list(state.subscribers_bytes):
                        try:
                            await q.put(data)
                        except Exception:
                            pass
                            
                except asyncio.TimeoutError:
                    continue
                except OSError:
                    break
                except Exception:
                    break
        
        state.stop.set()
        try:
            state.input_queue.put_nowait(None)
        except Exception:
            pass
        try:
            await asyncio.to_thread(os.close, state.master_fd)
        except Exception:
            pass

    async def _wait_fd_writable(self, fd: int) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def _mark_writable() -> None:
            loop.remove_writer(fd)
            if not future.done():
                future.set_result(None)

        loop.add_writer(fd, _mark_writable)
        try:
            await future
        finally:
            if not future.done():
                loop.remove_writer(fd)

    async def _write_fd_all(self, fd: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            try:
                written = os.write(fd, view)
                if written <= 0:
                    raise RuntimeError("PTY write returned no bytes")
                view = view[written:]
            except BlockingIOError:
                await self._wait_fd_writable(fd)
            except InterruptedError:
                continue

    def _fail_pending_pty_writes(self, state: PTYState, message: str) -> None:
        while True:
            try:
                request = state.input_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if request is None:
                continue
            _, done = request
            if not done.done():
                done.set_exception(RuntimeError(message))

    async def _pty_writer(self, state: PTYState) -> None:
        failure_message = f"PTY input unavailable for shell {state.shell_id or '?'}"
        try:
            while not state.stop.is_set():
                request = await state.input_queue.get()
                if request is None:
                    break
                data, done = request
                try:
                    await self._write_fd_all(state.master_fd, data)
                except asyncio.CancelledError:
                    if not done.done():
                        done.set_exception(RuntimeError(failure_message))
                    raise
                except Exception as exc:
                    failure_message = f"PTY write failed for shell {state.shell_id or '?'}: {exc}"
                    if not done.done():
                        done.set_exception(RuntimeError(failure_message))
                    break
                else:
                    if not done.done():
                        done.set_result(None)
        finally:
            self._fail_pending_pty_writes(state, failure_message)

    async def _write_live_pty_state(self, state: PTYState, data: str) -> None:
        if state.stop.is_set():
            raise RuntimeError(f"PTY input unavailable for shell {state.shell_id or '?'}")
        writer = state.writer
        if writer is None or writer.done():
            raise RuntimeError(f"PTY input unavailable for shell {state.shell_id or '?'}")
        encoded = data.encode("utf-8")
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        request: PTYWriteRequest = (encoded, done)
        state.input_queue.put_nowait(request)
        await done

    async def _is_pid_alive(self, pid: Optional[int]) -> bool:
        if not pid: return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    async def is_pid_alive(self, pid: Optional[int]) -> bool:
        return await self._is_pid_alive(pid)

    async def _mark_exited(self, record: ShellRecord, exit_code: Optional[int]) -> None:
        last_pid = record.pid
        record.pid = None
        record.status = "exited"
        record.exit_code = exit_code
        record.updated_at = time.time()
        await self._save_record(record)
        await self._prune_exited_shells_locked(max_count=self.MAX_EXITED_SHELLS)
        await self._emit(EventType.SHELL_EXITED, record, exit_code=exit_code)
        self._run_hook_exited(record, last_pid)

    async def _collect_exit_code(self, pid: Optional[int]) -> Optional[int]:
        if not pid: return None
        try:
            waited_pid, status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == 0: return None
            if os.WIFEXITED(status): return os.WEXITSTATUS(status)
            if os.WIFSIGNALED(status): return -os.WTERMSIG(status)
        except Exception:
            return None
        return None

    async def _stop_pty(self, shell_id: str) -> None:
        state = self._pty.pop(shell_id, None)
        if not state: return
        state.stop.set()
        try:
            state.input_queue.put_nowait(None)
        except Exception:
            pass
        if state.reader:
            state.reader.cancel()
        if state.writer and state.writer is not asyncio.current_task():
            state.writer.cancel()
        
        # If proxy pid exists, kill it (detach)
        if state.proxy_pid:
            try:
                os.kill(state.proxy_pid, signal.SIGTERM)
            except Exception:
                pass

    def _backend_name(self, record: ShellRecord) -> str:
        return normalize_backend(
            getattr(record, "backend", None),
            uses_pty=bool(getattr(record, "uses_pty", False)),
            uses_pipes=bool(getattr(record, "uses_pipes", False)),
            uses_dtach=bool(getattr(record, "uses_dtach", False)),
        )

    async def _live_stream_reader(
        self,
        record: ShellRecord,
        state: PipeState,
        *,
        stream_name: str,
        stream: asyncio.StreamReader | None,
        log_path: Path,
    ) -> None:
        if stream is None:
            return

        async with aiofiles.open(log_path, "ab") as log_fh:
            pending_flush_bytes = 0
            while not state.stop.is_set():
                try:
                    data = await asyncio.wait_for(
                        stream.read(self.PIPE_READ_CHUNK_BYTES),
                        timeout=self.PIPE_LOG_FLUSH_INTERVAL_SECONDS,
                    )
                    if not data:
                        break

                    await log_fh.write(data)
                    pending_flush_bytes += len(data)
                    if pending_flush_bytes >= self.PIPE_LOG_FLUSH_BYTES:
                        await log_fh.flush()
                        pending_flush_bytes = 0

                    await self._dispatch_live_chunk(record, state, stream_name, data)

                except asyncio.TimeoutError:
                    if pending_flush_bytes > 0:
                        await log_fh.flush()
                        pending_flush_bytes = 0
                except asyncio.CancelledError:
                    break
                except Exception:
                    break
            if pending_flush_bytes > 0:
                await log_fh.flush()

    async def _dispatch_live_chunk(
        self,
        record: ShellRecord,
        state: PipeState,
        stream_name: str,
        data: bytes,
    ) -> None:
        if stream_name == "stderr":
            text_subscribers = list(state.stderr_subscribers)
            bytes_subscribers = list(state.stderr_subscribers_bytes)
        else:
            text_subscribers = list(state.stdout_subscribers)
            bytes_subscribers = list(state.stdout_subscribers_bytes)
        should_publish_log_chunk = self._event_bus.has_subscribers()
        text: str | None = None

        if should_publish_log_chunk or text_subscribers:
            text = data.decode("utf-8", errors="replace")

        if should_publish_log_chunk and text is not None:
            event = ShellEvent(
                type=EventType.LOG_CHUNK,
                shell_id=record.id,
                data={"stream": stream_name, "chunk": text},
                app_id=record.app_id or record.derive_app_id(),
                parent_shell_id=record.parent_shell_id,
                is_app_worker=record.is_app_worker,
            )
            await self._event_bus.publish(event)

        if text is not None:
            for q in text_subscribers:
                try:
                    q.put_nowait(text)
                except Exception:
                    pass
        for q in bytes_subscribers:
            try:
                q.put_nowait(data)
            except Exception:
                pass

    def _pipe_stream_transport(self, stream: asyncio.StreamReader | None) -> _PipeReadTransport | None:
        if stream is None:
            return None
        transport = getattr(stream, "_transport", None)
        if transport is None:
            return None
        return cast(_PipeReadTransport, transport)

    def _pipe_stream_fd(self, stream: asyncio.StreamReader | None) -> int | None:
        transport = self._pipe_stream_transport(stream)
        if transport is None or not hasattr(transport, "get_extra_info"):
            return None
        for extra_info_key in ("pipe", "socket"):
            try:
                stream_obj = transport.get_extra_info(extra_info_key)
            except Exception:
                continue
            if stream_obj is None or not hasattr(stream_obj, "fileno"):
                continue
            try:
                return int(cast(_HasFileno, stream_obj).fileno())
            except Exception:
                continue
        return None

    def _remove_native_pipe_reader(self, state: PipeState) -> None:
        reader_fd = state.native_reader_fd
        if reader_fd is None:
            return
        try:
            asyncio.get_running_loop().remove_reader(reader_fd)
        except Exception:
            pass
        state.native_reader_fd = None

    def _on_native_pipe_stdout_ready(self, record: ShellRecord, state: PipeState) -> None:
        if state.stop.is_set():
            return

        native_pump = cast(Optional[NativePipePumpHandle], state.native_pump)
        queue = state.native_chunk_queue
        if native_pump is None or queue is None:
            return

        try:
            chunks = native_pump.read_available(self.PIPE_NATIVE_MAX_DRAIN_CHUNKS)
        except Exception:
            self._remove_native_pipe_reader(state)
            try:
                queue.put_nowait(None)
            except Exception:
                pass
            return

        for chunk in chunks:
            try:
                queue.put_nowait(chunk)
            except Exception:
                pass

        if native_pump.is_finished():
            self._remove_native_pipe_reader(state)
            try:
                queue.put_nowait(None)
            except Exception:
                pass

    async def _activate_native_pipe_stdout(
        self,
        record: ShellRecord,
        state: PipeState,
        *,
        read_chunk_bytes: int,
        log_flush_bytes: int,
        log_flush_interval_ms: int,
    ) -> bool:
        stream = state.process.stdout
        transport = self._pipe_stream_transport(stream)
        stdout_fd = self._pipe_stream_fd(stream)
        if stream is None or transport is None or stdout_fd is None:
            return False

        paused = False
        prebuffer = b""
        try:
            if hasattr(transport, "pause_reading"):
                transport.pause_reading()
                paused = True

            buffer_obj = getattr(stream, "_buffer", None)
            if isinstance(buffer_obj, (bytes, bytearray)):
                prebuffer = bytes(buffer_obj)
                if isinstance(buffer_obj, bytearray):
                    try:
                        buffer_obj.clear()
                    except Exception:
                        pass

            if prebuffer:
                async with aiofiles.open(record.stdout_log, "ab") as log_fh:
                    await log_fh.write(prebuffer)
                    await log_fh.flush()

            native_pump = create_native_pipe_pump(
                stdout_fd=stdout_fd,
                log_path=record.stdout_log,
                read_chunk_bytes=read_chunk_bytes,
                log_flush_bytes=log_flush_bytes,
                log_flush_interval_ms=log_flush_interval_ms,
            )
            if native_pump is None:
                if paused and hasattr(transport, "resume_reading"):
                    transport.resume_reading()
                return False

            reader_fd = int(native_pump.reader_fd())
            if reader_fd < 0:
                try:
                    await asyncio.to_thread(native_pump.stop)
                except Exception:
                    pass
                if paused and hasattr(transport, "resume_reading"):
                    transport.resume_reading()
                return False

            state.native_pump = native_pump
            state.native_engine = "native-pipe"
            state.native_phase = native_extension_phase()
            state.native_reader_fd = reader_fd
            state.native_chunk_queue = AsyncQueue()
            if prebuffer:
                state.native_initial_chunks.append(prebuffer)
            asyncio.get_running_loop().add_reader(
                reader_fd,
                self._on_native_pipe_stdout_ready,
                record,
                state,
            )
            state.stdout_reader = asyncio.create_task(self._native_pipe_stdout_relay(record, state))
            return True
        except Exception:
            self._remove_native_pipe_reader(state)
            state.native_chunk_queue = None
            native_pump = cast(Optional[NativePipePumpHandle], state.native_pump)
            if native_pump is not None:
                try:
                    await asyncio.to_thread(native_pump.stop)
                except Exception:
                    pass
            state.native_pump = None
            state.native_engine = None
            state.native_phase = None
            if paused and hasattr(transport, "resume_reading"):
                try:
                    transport.resume_reading()
                except Exception:
                    pass
            return False

    async def _native_pipe_stdout_relay(self, record: ShellRecord, state: PipeState) -> None:
        native_pump = cast(Optional[NativePipePumpHandle], state.native_pump)
        if native_pump is None:
            return

        try:
            if state.native_initial_chunks:
                initial_chunks = list(state.native_initial_chunks)
                state.native_initial_chunks.clear()
                for chunk in initial_chunks:
                    await self._dispatch_live_chunk(record, state, "stdout", chunk)

            queue = state.native_chunk_queue
            if queue is None:
                return

            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                await self._dispatch_live_chunk(record, state, "stdout", chunk)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            self._remove_native_pipe_reader(state)
            state.native_chunk_queue = None
            state.native_initial_chunks.clear()
            if state.native_pump is native_pump:
                state.native_pump = None
            try:
                await asyncio.to_thread(native_pump.stop)
            except Exception:
                pass

    async def _pipe_waiter(self, record: ShellRecord, state: PipeState) -> None:
        proc = state.process
        try:
            exit_code = await proc.wait()
        except asyncio.CancelledError:
            return
        except Exception:
            exit_code = proc.returncode

        async with self._get_lock():
            current = self._pipes.get(record.id)
            if current is state:
                self._pipes.pop(record.id, None)

        rec = await self._load_record(record.id)
        if rec and getattr(rec, "status", None) != "exited":
            await self._mark_exited(rec, exit_code)

    async def _launch_pipe(
        self,
        record: ShellRecord,
        *,
        pipe_config: dict[str, object] | None = None,
    ) -> ShellRecord:
        """Launch shell with live stdin/stdout pipes for bidirectional streaming."""
        record.set_backend(BACKEND_PIPE)
        env = self._prepare_env(record)
        resolved_pipe_config = normalize_pipe_config(pipe_config)
        launch_command = list(record.command)
        native_terminal_mode_requested = (
            resolved_pipe_config.mode == NATIVE_TERMINAL_PIPE_TESTING_MODE
        )
        python_terminal_mode_requested = (
            resolved_pipe_config.mode == PYTHON_TERMINAL_PIPE_TESTING_MODE
        )
        native_terminal_resolution = resolve_native_terminal_broker_command(launch_command)
        native_terminal_mode_active = False
        terminal_python_mode_active = False
        terminal_fallback_command: list[str] | None = None
        if python_terminal_mode_requested:
            launch_command = resolve_python_terminal_broker_command()
            terminal_python_mode_active = True
        elif native_terminal_mode_requested:
            if native_terminal_resolution.engine:
                launch_command = list(native_terminal_resolution.command)
                native_terminal_mode_active = True
            else:
                terminal_fallback_command = resolve_terminal_broker_fallback_command(
                    resolved_pipe_config.terminal_fallback,
                    launch_command,
                )
                if terminal_fallback_command is not None:
                    launch_command = list(terminal_fallback_command)
                else:
                    raise RuntimeError(
                        f"native terminal broker unavailable for pipe.mode={NATIVE_TERMINAL_PIPE_TESTING_MODE} "
                        f"with pipe.terminal_fallback={resolved_pipe_config.terminal_fallback!r}; "
                        "set FRAMEWORK_SHELLS_NATIVE_TERMINAL_BROKER, "
                        f"put {NATIVE_TERMINAL_BROKER_BIN} on PATH, or choose a usable fallback"
                    )

        await self._emit(EventType.SHELL_CREATED, record)

        proc = await asyncio.create_subprocess_exec(
            *launch_command,
            cwd=record.cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

        record.pid = proc.pid
        record.status = "running"
        record.updated_at = time.time()
        await self._save_record(record)
        await self._emit(EventType.SHELL_SPAWNED, record)
        self._run_hook_running(record)

        state = PipeState(
            process=proc,
            label=record.label,
            shell_id=record.id,
        )
        if native_terminal_mode_requested or python_terminal_mode_requested:
            if native_terminal_mode_active:
                state.native_engine = NATIVE_TERMINAL_PIPE_ENGINE
                state.native_phase = "prototype"
                _shell_debug(
                    "native_terminal_pipe",
                    (
                        f"shell={record.id} activated {NATIVE_TERMINAL_PIPE_TESTING_MODE} "
                        f"source={native_terminal_resolution.source or 'unknown'}"
                    ),
                )
            elif terminal_python_mode_active:
                state.native_engine = PYTHON_TERMINAL_PIPE_ENGINE
                state.native_phase = "fallback"
                _shell_debug(
                    "native_terminal_pipe",
                    (
                        f"shell={record.id} activated {PYTHON_TERMINAL_PIPE_TESTING_MODE} "
                        "source=python-module"
                    ),
                )
            else:
                fallback_desc = resolved_pipe_config.terminal_fallback
                if terminal_fallback_command is not None and terminal_fallback_command == launch_command:
                    if is_native_terminal_placeholder_command(record.command):
                        fallback_desc = "python_pty"
                    elif resolved_pipe_config.terminal_fallback == "command":
                        fallback_desc = "command"
                if terminal_fallback_command == resolve_python_terminal_broker_command():
                    state.native_engine = PYTHON_TERMINAL_PIPE_ENGINE
                    state.native_phase = "fallback"
                _shell_debug(
                    "native_terminal_pipe",
                    (
                        f"shell={record.id} requested {NATIVE_TERMINAL_PIPE_TESTING_MODE} "
                        f"but using terminal fallback {fallback_desc}"
                    ),
                )
        native_mode_requested = resolved_pipe_config.mode == NATIVE_PIPE_TESTING_MODE
        native_mode_active = False
        if native_mode_requested:
            if native_extension_available():
                native_mode_active = await self._activate_native_pipe_stdout(
                    record,
                    state,
                    read_chunk_bytes=resolved_pipe_config.read_chunk_bytes or self.PIPE_READ_CHUNK_BYTES,
                    log_flush_bytes=resolved_pipe_config.log_flush_bytes or self.PIPE_LOG_FLUSH_BYTES,
                    log_flush_interval_ms=resolved_pipe_config.log_flush_interval_ms or int(self.PIPE_LOG_FLUSH_INTERVAL_SECONDS * 1000),
                )
                if native_mode_active:
                    _shell_debug(
                        "native_pipe",
                        f"shell={record.id} activated native_pipe_testing phase={native_extension_phase() or 'unknown'}",
                    )
            if not native_mode_active:
                _shell_debug(
                    "native_pipe",
                    f"shell={record.id} requested native_pipe_testing but using Python pipe pump",
                )

        if not native_mode_active:
            state.stdout_reader = asyncio.create_task(
                self._live_stream_reader(
                    record,
                    state,
                    stream_name="stdout",
                    stream=proc.stdout,
                    log_path=Path(record.stdout_log),
                )
            )
        state.stderr_reader = asyncio.create_task(
            self._live_stream_reader(
                record,
                state,
                stream_name="stderr",
                stream=proc.stderr,
                log_path=Path(record.stderr_log),
            )
        )
        state.waiter = asyncio.create_task(self._pipe_waiter(record, state))
        self._pipes[record.id] = state
        return record

    async def _stop_pipe(self, shell_id: str) -> None:
        state = self._pipes.pop(shell_id, None)
        if not state: return
        state.stop.set()
        self._remove_native_pipe_reader(state)
        queue = state.native_chunk_queue
        if queue is not None:
            try:
                queue.put_nowait(None)
            except Exception:
                pass
        if state.stdout_reader:
            state.stdout_reader.cancel()
            if state.stdout_reader is not asyncio.current_task():
                try:
                    await state.stdout_reader
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        if state.stderr_reader:
            state.stderr_reader.cancel()
            if state.stderr_reader is not asyncio.current_task():
                try:
                    await state.stderr_reader
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        native_pump = cast(Optional[NativePipePumpHandle], state.native_pump)
        if native_pump is not None:
            try:
                await asyncio.to_thread(native_pump.stop)
            except Exception:
                pass
            state.native_pump = None
        if state.waiter and state.waiter is not asyncio.current_task():
            state.waiter.cancel()
        proc = state.process
        # Close stdin to signal EOF
        if proc.stdin and not proc.stdin.is_closing():
            proc.stdin.close()
            try:
                await proc.stdin.wait_closed()
            except Exception:
                pass


    # Public methods
    
    async def spawn_shell(
        self,
        command: Iterable[str],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        spec_id: Optional[str] = None,
        subgroups: Optional[List[str]] = None,
        ui: Optional[dict[str, object]] = None,
        autostart: bool = True,
    ) -> ShellRecord:
        record = self._create_record(
            command, cwd=cwd, env=env, label=label,
            spec_id=spec_id, subgroups=subgroups, ui=ui, autostart=autostart,
            backend=BACKEND_PROC
        )
        if autostart:
            await self._launch(record)
        else:
            await self._save_record(record)
            await self._emit(EventType.SHELL_CREATED, record)
        return record

    async def spawn_shell_pty(
        self,
        command: Iterable[str],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        spec_id: Optional[str] = None,
        subgroups: Optional[List[str]] = None,
        ui: Optional[dict[str, object]] = None,
        pty_mode: Optional[str] = None,
        autostart: bool = True,
        parent_shell_id: Optional[str] = None,
    ) -> ShellRecord:
        record = self._create_record(
            command, cwd=cwd, env=env, label=label,
            spec_id=spec_id, subgroups=subgroups, ui=ui, autostart=autostart,
            backend=BACKEND_PTY, pty_mode=pty_mode, parent_shell_id=parent_shell_id
        )
        if autostart:
            await self._launch_pty(record)
        else:
            await self._save_record(record)
            await self._emit(EventType.SHELL_CREATED, record)
        return record

    async def spawn_shell_pipe(
        self,
        command: Iterable[str],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        spec_id: Optional[str] = None,
        subgroups: Optional[List[str]] = None,
        ui: Optional[dict[str, object]] = None,
        pipe_config: dict[str, object] | None = None,
        autostart: bool = True,
        parent_shell_id: Optional[str] = None,
    ) -> ShellRecord:
        record = self._create_record(
            command, cwd=cwd, env=env, label=label,
            spec_id=spec_id, subgroups=subgroups, ui=ui, autostart=autostart,
            backend=BACKEND_PIPE,
            parent_shell_id=parent_shell_id
        )
        if autostart:
            await self._launch_pipe(record, pipe_config=pipe_config)
        else:
            await self._save_record(record)
            await self._emit(EventType.SHELL_CREATED, record)
        return record
    
    async def spawn_shell_dtach(
        self,
        command: Iterable[str],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        label: Optional[str] = None,
        spec_id: Optional[str] = None,
        subgroups: Optional[List[str]] = None,
        ui: Optional[dict[str, object]] = None,
        pty_mode: Optional[str] = None,
        autostart: bool = True,
        parent_shell_id: Optional[str] = None,
    ) -> ShellRecord:
        # Deprecated compatibility alias: new dtach requests launch as PTY.
        return await self.spawn_shell_pty(
            command=command,
            cwd=cwd,
            env=env,
            label=label,
            spec_id=spec_id,
            subgroups=subgroups,
            ui=ui,
            pty_mode=pty_mode,
            autostart=autostart,
            parent_shell_id=parent_shell_id,
        )

    async def list_shells(self) -> List[ShellRecord]:
        async with self._get_lock():
            await self._adopt_orphaned_shells()
            records: list[ShellRecord] = []
            async for record in self._aiter_records():
                records.append(record)
            return sorted(records, key=lambda rec: rec.created_at)

    async def get_shell(self, shell_id: str) -> Optional[ShellRecord]:
        async with self._get_lock():
            await self._adopt_orphaned_shells()
            return await self._load_record(shell_id)

    async def load_shell_record(self, shell_id: str) -> Optional[ShellRecord]:
        """Load a shell record from disk without adoption or runtime refresh."""
        return await self._load_record(shell_id)

    async def find_shell_by_label(self, label: str, status: Optional[str] = "running") -> Optional[ShellRecord]:
        if not label: return None
        async with self._get_lock():
            await self._adopt_orphaned_shells()
            async for record in self._aiter_records():
                if record.label != label: continue
                if status and record.status != status: continue
                if status == "running" and not await self._is_pid_alive(record.pid): continue
                return record
        return None

    def _signal_pid_or_pgrp(self, pid: int, sig: signal.Signals) -> None:
        """Best-effort signal delivery: prefer process group, fallback to PID."""
        try:
            pgid = os.getpgid(int(pid))
            os.killpg(pgid, sig)
            return
        except Exception:
            pass
        try:
            os.kill(int(pid), sig)
        except Exception:
            pass

    async def terminate_shell(self, shell_id: str, force: bool = False) -> None:
        rec = await self._load_record(shell_id)
        if not rec: return
        
        # Dtach shells need SIGKILL - bash inside dtach ignores SIGTERM
        if rec.uses_dtach:
            force = True
        
        sig = signal.SIGKILL if force else signal.SIGTERM
        
        # For dtach shells, we need to kill the dtach master process
        # The socket file tells us if dtach is involved
        if rec.uses_dtach:
            socket_path = self.sockets_dir / f"{shell_id}.sock"
            pid_file = self.sockets_dir / f"{shell_id}.pid"
            
            # Kill the shell process (and its process group) inside dtach.
            if rec.pid:
                self._signal_pid_or_pgrp(rec.pid, sig)
            
            # Find and kill dtach master by checking who owns the socket
            # Or just remove the socket to force dtach to exit
            if socket_path.exists():
                try:
                    socket_path.unlink()
                except Exception:
                    pass
            if pid_file.exists():
                try:
                    pid_file.unlink()
                except Exception:
                    pass
        elif rec.pid:
            # Regular process - signal process group (preferred) so children die too.
            self._signal_pid_or_pgrp(rec.pid, sig)
        
        # Stop any PTY/pipe proxies we're running
        await self._stop_pty(shell_id)
        await self._stop_pipe(shell_id)
        
        await asyncio.sleep(0.1)
        if rec.pid and not await self._is_pid_alive(rec.pid):
             code = await self._collect_exit_code(rec.pid)
             await self._mark_exited(rec, code)

    async def remove_shell(self, shell_id: str, force: bool = False) -> bool:
        """Terminate shell and remove its metadata/logs."""
        rec = await self._load_record(shell_id)
        await self.terminate_shell(shell_id, force=force)
        
        # Remove metadata
        meta_dir = self.metadata_dir / shell_id
        if meta_dir.exists():
            await asyncio.to_thread(shutil.rmtree, meta_dir, ignore_errors=True)
        
        # Remove logs
        if rec:
            for log_path in [rec.stdout_log, rec.stderr_log]:
                p = Path(log_path)
                if p.exists():
                    try:
                        await asyncio.to_thread(p.unlink)
                    except Exception:
                        pass
            # Emit removed event
            await self._emit(EventType.SHELL_REMOVED, rec)
        
        return True

    # ------------------------------------------------------------------
    # Describe / stats

    LOG_TAIL_BYTES: int = 4096
    MAX_EXITED_SHELLS: int = 50
    PIPE_READ_CHUNK_BYTES: int = 64 * 1024
    PIPE_LOG_FLUSH_BYTES: int = 256 * 1024
    PIPE_LOG_FLUSH_INTERVAL_SECONDS: float = 0.25
    PIPE_NATIVE_WAIT_TIMEOUT_MS: int = 50
    PIPE_NATIVE_MAX_DRAIN_CHUNKS: int = 64

    async def describe(
        self,
        record: ShellRecord,
        *,
        include_logs: bool = False,
        tail_lines: int = 0,
    ) -> JSONMap:
        """Return a dict with shell payload, stats, and optionally logs."""
        payload = record.to_payload()
        payload["capabilities"] = await self.get_shell_capabilities(record)
        payload["stats"] = await self._process_stats(record)
        pipe_runtime = self._pipe_runtime_payload(record)
        if pipe_runtime:
            payload["pipe_runtime"] = pipe_runtime
        if include_logs:
            payload["logs"] = {
                "stdout_tail": await self._read_log_tail(Path(record.stdout_log), tail_lines),
                "stderr_tail": await self._read_log_tail(Path(record.stderr_log), tail_lines),
            }
        return payload

    def _pipe_runtime_payload(self, record: ShellRecord) -> JSONMap | None:
        if self._backend_name(record) != BACKEND_PIPE:
            return None
        state = self._pipes.get(record.id)
        if state is None or not state.native_engine:
            return None
        payload: JSONMap = {
            "engine": str(state.native_engine),
            "active": bool(
                state.native_pump is not None
                or state.native_engine in {NATIVE_TERMINAL_PIPE_ENGINE, PYTHON_TERMINAL_PIPE_ENGINE}
            ),
        }
        phase = state.native_phase or native_extension_phase()
        if phase:
            payload["phase"] = phase
        return payload

    async def _process_stats(self, record: ShellRecord) -> JSONMap:
        stats: JSONMap = {
            "alive": False,
            "uptime": None,
        }
        if record.pid:
            alive = await self._is_pid_alive(record.pid)
            stats["alive"] = alive
            if alive:
                stats["uptime"] = max(0.0, time.time() - record.created_at)
                if psutil:
                    try:
                        proc = await asyncio.to_thread(psutil.Process, record.pid)
                        with proc.oneshot():
                            stats["cpu_percent"] = proc.cpu_percent(interval=0.0)
                            stats["memory_rss"] = proc.memory_info().rss
                            stats["num_threads"] = proc.num_threads()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                else:
                    try:
                        ps_output = await asyncio.to_thread(
                            subprocess.run,
                            ["ps", "-p", str(record.pid), "-o", "%cpu=,%mem=,rss=,nlwp="],
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                        parts = ps_output.stdout.strip().split()
                        if len(parts) >= 4:
                            stats["cpu_percent"] = float(parts[0])
                            stats["memory_rss"] = int(float(parts[2]) * 1024)
                            stats["num_threads"] = int(parts[3])
                    except Exception:
                        pass
        return stats

    async def _read_log_tail(self, path: Path, lines: int) -> List[str]:
        if lines <= 0 or not path.exists():
            return []
        size = await asyncio.to_thread(path.stat)
        to_read = min(size.st_size, self.LOG_TAIL_BYTES)
        async with aiofiles.open(path, "rb") as fh:
            await fh.seek(-to_read, os.SEEK_END)
            data = await fh.read()
            decoded_data = data.decode("utf-8", errors="replace")
        return decoded_data.splitlines(keepends=True)[-lines:]

    async def _prune_exited_shells_locked(self, *, max_count: int = 50) -> JSONMap:
        records: List[ShellRecord] = []
        async for record in self._aiter_records():
            records.append(record)

        exited = [rec for rec in records if (getattr(rec, "status", None) or "") == "exited"]
        if max_count < 0:
            max_count = 0
        if len(exited) <= max_count:
            return {"kept": len(exited), "purged": 0, "removed_ids": []}

        exited.sort(key=lambda rec: (float(getattr(rec, "updated_at", 0) or 0), float(getattr(rec, "created_at", 0) or 0)))
        to_remove = exited[:-max_count]
        removed_ids: List[str] = []

        for rec in to_remove:
            meta_dir = self.metadata_dir / rec.id
            if meta_dir.exists():
                await asyncio.to_thread(shutil.rmtree, meta_dir, ignore_errors=True)
            for log_path in [rec.stdout_log, rec.stderr_log]:
                try:
                    path = Path(log_path)
                    if path.exists():
                        await asyncio.to_thread(path.unlink)
                except Exception:
                    pass
            removed_ids.append(rec.id)
            try:
                await self._emit(EventType.SHELL_REMOVED, rec)
            except Exception:
                pass

        return {"kept": max_count, "purged": len(removed_ids), "removed_ids": removed_ids}

    async def prune_exited_shells(self, *, max_count: int = 50) -> JSONMap:
        """Keep only the newest exited shell records, removing older metadata/logs."""
        async with self._get_lock():
            return await self._prune_exited_shells_locked(max_count=max_count)

    async def _prune_exited_logs_locked(self, *, max_count: int = 50) -> JSONMap:
        records: List[ShellRecord] = []
        async for record in self._aiter_records():
            records.append(record)

        exited = [rec for rec in records if (getattr(rec, "status", None) or "") == "exited"]
        if max_count < 0:
            max_count = 0
        if len(exited) <= max_count:
            return {"kept": len(exited), "trimmed_logs": 0, "shell_ids": []}

        exited.sort(
            key=lambda rec: (
                float(getattr(rec, "updated_at", 0) or 0),
                float(getattr(rec, "created_at", 0) or 0),
            ),
            reverse=True,
        )
        to_trim = exited[max_count:]
        trimmed_shell_ids: List[str] = []

        for rec in to_trim:
            trimmed_any = False
            for log_path in [rec.stdout_log, rec.stderr_log]:
                try:
                    path = Path(log_path)
                    if path.exists():
                        await asyncio.to_thread(path.unlink)
                        trimmed_any = True
                except Exception:
                    pass
            if trimmed_any:
                trimmed_shell_ids.append(rec.id)

        return {"kept": max_count, "trimmed_logs": len(trimmed_shell_ids), "shell_ids": trimmed_shell_ids}

    async def prune_exited_logs(self, *, max_count: int = 50) -> JSONMap:
        """Keep only the newest exited shell log files, preserving metadata records."""
        async with self._get_lock():
            return await self._prune_exited_logs_locked(max_count=max_count)

    async def _log_stream_payload(
        self,
        path: Path,
        *,
        lines: Optional[List[str]] = None,
        extra: Mapping[str, object] | None = None,
    ) -> JSONMap:
        exists = path.exists()
        stat = await asyncio.to_thread(path.stat) if exists else None
        mtime = float(stat.st_mtime) if stat else None
        size = int(stat.st_size) if stat else 0
        age_seconds = max(0.0, time.time() - mtime) if mtime is not None else None
        payload: JSONMap = {
            "path": str(path),
            "mtime": mtime,
            "size": size,
            "age_seconds": age_seconds,
        }
        if lines is not None:
            payload["lines"] = lines
        if extra:
            payload.update(extra)
        return payload

    async def get_log_tail(
        self,
        shell_id: str,
        *,
        stream: str = "both",
        lines: int = 200,
    ) -> JSONMap:
        rec = await self.get_shell(shell_id)
        if not rec:
            raise KeyError(f"Shell not found: {shell_id}")

        stream_name = (stream or "both").strip().lower()
        if stream_name not in {"stdout", "stderr", "both"}:
            raise ValueError(f"Invalid stream: {stream}")

        result: JSONMap = {
            "shell_id": shell_id,
            "created_at": rec.created_at,
            "updated_at": rec.updated_at,
            "status": rec.status,
        }
        if stream_name in {"stdout", "both"}:
            stdout_path = Path(rec.stdout_log)
            stdout_window = await read_event_window(
                stdout_path,
                lines=max(0, int(lines)),
                max_bytes=self.LOG_TAIL_BYTES,
            )
            stdout_records = cast(list[dict[str, object]], stdout_window["records"])
            result["stdout"] = await self._log_stream_payload(
                stdout_path,
                lines=[str(record.get("text") or "") for record in stdout_records],
                extra={
                    "byte_window_start": stdout_window["byte_window_start"],
                    "byte_window_end": stdout_window["byte_window_end"],
                    "partial_head": stdout_window["partial_head"],
                    "truncated": stdout_window["truncated"],
                    "event_count": stdout_window["event_count"],
                },
            )
        if stream_name in {"stderr", "both"}:
            stderr_path = Path(rec.stderr_log)
            stderr_window = await read_event_window(
                stderr_path,
                lines=max(0, int(lines)),
                max_bytes=self.LOG_TAIL_BYTES,
            )
            stderr_records = cast(list[dict[str, object]], stderr_window["records"])
            result["stderr"] = await self._log_stream_payload(
                stderr_path,
                lines=[str(record.get("text") or "") for record in stderr_records],
                extra={
                    "byte_window_start": stderr_window["byte_window_start"],
                    "byte_window_end": stderr_window["byte_window_end"],
                    "partial_head": stderr_window["partial_head"],
                    "truncated": stderr_window["truncated"],
                    "event_count": stderr_window["event_count"],
                },
            )
        return result

    async def _search_log_file(
        self,
        path: Path,
        *,
        query: str,
        limit: int,
        regex: bool,
        ignore_case: bool,
    ) -> JSONList:
        if not query or limit <= 0 or not path.exists():
            return []

        flags = re.IGNORECASE if ignore_case else 0
        matcher = re.compile(query, flags) if regex else None
        matches: JSONList = []

        async with aiofiles.open(path, "r", encoding="utf-8", errors="replace") as fh:
            line_number = 0
            async for line in fh:
                line_number += 1
                text = line.rstrip("\n")
                haystack = text.lower() if ignore_case and not regex else text
                needle = query.lower() if ignore_case and not regex else query
                matched = bool(matcher.search(text)) if matcher else (needle in haystack)
                if not matched:
                    continue
                matches.append({"line_number": line_number, "text": text})
                if len(matches) >= limit:
                    break
        return matches

    async def search_logs(
        self,
        shell_id: str,
        *,
        stream: str = "both",
        query: str,
        limit: int = 100,
        regex: bool = False,
        ignore_case: bool = False,
    ) -> JSONMap:
        rec = await self.get_shell(shell_id)
        if not rec:
            raise KeyError(f"Shell not found: {shell_id}")

        stream_name = (stream or "both").strip().lower()
        if stream_name not in {"stdout", "stderr", "both"}:
            raise ValueError(f"Invalid stream: {stream}")

        clamped_limit = max(1, min(int(limit), 1000))
        result: JSONMap = {
            "shell_id": shell_id,
            "created_at": rec.created_at,
            "updated_at": rec.updated_at,
            "status": rec.status,
            "stream": stream_name,
            "query": query,
            "regex": bool(regex),
            "ignore_case": bool(ignore_case),
        }

        if stream_name in {"stdout", "both"}:
            stdout_path = Path(rec.stdout_log)
            result["stdout"] = await self._log_stream_payload(stdout_path)
            result["stdout"]["matches"] = await self._search_log_file(
                stdout_path,
                query=query,
                limit=clamped_limit,
                regex=regex,
                ignore_case=ignore_case,
            )

        if stream_name in {"stderr", "both"}:
            stderr_path = Path(rec.stderr_log)
            result["stderr"] = await self._log_stream_payload(stderr_path)
            result["stderr"]["matches"] = await self._search_log_file(
                stderr_path,
                query=query,
                limit=clamped_limit,
                regex=regex,
                ignore_case=ignore_case,
            )

        return result

    async def inspect_logs(
        self,
        shell_id: str,
        *,
        stream: str = "both",
        lines: int = 200,
        query: Optional[str] = None,
        exclude_query: Optional[str] = None,
        regex: bool = False,
        ignore_case: bool = False,
        format: Optional[str] = None,
        signature: Optional[str] = None,
        exclude_signature: Optional[str] = None,
    ) -> JSONMap:
        rec = await self.get_shell(shell_id)
        if not rec:
            raise KeyError(f"Shell not found: {shell_id}")

        stream_name = (stream or "both").strip().lower()
        if stream_name not in {"stdout", "stderr", "both"}:
            raise ValueError(f"Invalid stream: {stream}")

        format_name = (format or "").strip().lower() or None
        if format_name not in {None, PLAIN_FORMAT, JSON_FORMAT, JSONRPC_FORMAT}:
            raise ValueError(f"Invalid format: {format}")

        signature_value = str(signature or "").strip() or None
        line_count = max(0, int(lines))

        exclude_signature_value = str(exclude_signature or "").strip() or None
        result: JSONMap = {
            "shell_id": shell_id,
            "created_at": rec.created_at,
            "updated_at": rec.updated_at,
            "status": rec.status,
            "stream": stream_name,
            "query": query,
            "exclude_query": exclude_query,
            "regex": bool(regex),
            "ignore_case": bool(ignore_case),
            "format": format_name,
            "signature": signature_value,
            "exclude_signature": exclude_signature_value,
        }

        if stream_name in {"stdout", "both"}:
            stdout_path = Path(rec.stdout_log)
            stdout_inspection = await inspect_log_file(
                stdout_path,
                stream="stdout",
                lines=line_count,
                max_bytes=self.LOG_TAIL_BYTES,
                query=query,
                exclude_query=exclude_query,
                regex=regex,
                ignore_case=ignore_case,
                format_filter=format_name,
                signature_filter=signature_value,
                exclude_signature=exclude_signature_value,
            )
            result["stdout"] = await self._log_stream_payload(
                stdout_path,
                extra=stdout_inspection,
            )

        if stream_name in {"stderr", "both"}:
            stderr_path = Path(rec.stderr_log)
            stderr_inspection = await inspect_log_file(
                stderr_path,
                stream="stderr",
                lines=line_count,
                max_bytes=self.LOG_TAIL_BYTES,
                query=query,
                exclude_query=exclude_query,
                regex=regex,
                ignore_case=ignore_case,
                format_filter=format_name,
                signature_filter=signature_value,
                exclude_signature=exclude_signature_value,
            )
            result["stderr"] = await self._log_stream_payload(
                stderr_path,
                extra=stderr_inspection,
            )

        return result

    # ------------------------------------------------------------------
    # PTY / pipe I/O methods

    def get_pipe_state(self, shell_id: str) -> Optional[PipeState]:
        """Return the live PipeState for a running pipe-backed shell, if present.

        Note: raw pipe I/O currently has no manager-adoption path after a manager
        restart, so this is only available in the process that currently owns
        the live pipe state.
        """
        return self._pipes.get(shell_id)

    async def get_shell_capabilities(self, record_or_shell_id: ShellRecord | str) -> JSONMap:
        if isinstance(record_or_shell_id, str):
            record = await self.get_shell(record_or_shell_id)
            if record is None:
                raise KeyError(f"Shell not found: {record_or_shell_id}")
        else:
            record = record_or_shell_id

        backend = self._backend_name(record)
        async with self._get_lock():
            has_pty = record.id in self._pty
            pipe_state = self._pipes.get(record.id)
            has_pipe = pipe_state is not None and pipe_state.process.returncode is None

        if backend == "dtach":
            return {
                "backend": backend,
                "stdin_write": has_pty,
                "stdin_eof": False,
                "stdout_subscribe": has_pty,
                "stdout_subscribe_bytes": has_pty,
                "stderr_subscribe": False,
                "resize": has_pty,
                "reattach": True,
            }
        if backend == "pty":
            return {
                "backend": backend,
                "stdin_write": has_pty,
                "stdin_eof": False,
                "stdout_subscribe": has_pty,
                "stdout_subscribe_bytes": has_pty,
                "stderr_subscribe": False,
                "resize": has_pty,
                "reattach": False,
            }
        if backend == "pipe":
            return {
                "backend": backend,
                "stdin_write": has_pipe and pipe_state.stdin_supported if pipe_state is not None else False,
                "stdin_eof": has_pipe and pipe_state.stdin_supported if pipe_state is not None else False,
                "stdout_subscribe": has_pipe,
                "stdout_subscribe_bytes": has_pipe,
                "stderr_subscribe": has_pipe,
                "resize": False,
                "reattach": False,
            }
        if backend == "proc":
            return {
                "backend": backend,
                "stdin_write": False,
                "stdin_eof": False,
                "stdout_subscribe": has_pipe,
                "stdout_subscribe_bytes": has_pipe,
                "stderr_subscribe": has_pipe,
                "resize": False,
                "reattach": False,
            }
        return {
            "backend": backend,
            "stdin_write": False,
            "stdin_eof": False,
            "stdout_subscribe": False,
            "stdout_subscribe_bytes": False,
            "stderr_subscribe": False,
            "resize": False,
            "reattach": False,
        }


    async def subscribe_output_stream(self, shell_id: str, stream_name: str = "stdout") -> AsyncQueue[str]:
        """Subscribe to live shell output text for a specific stream."""
        async with self._get_lock():
            state = self._pty.get(shell_id)
            if state:
                if stream_name != "stdout":
                    raise KeyError(f"No live {stream_name} stream for shell {shell_id}")
                q: AsyncQueue[str] = AsyncQueue()
                state.subscribers.append(q)
                return q
            pipe_state = self._pipes.get(shell_id)
            if pipe_state:
                q = AsyncQueue()
                if stream_name == "stderr":
                    pipe_state.stderr_subscribers.append(q)
                else:
                    pipe_state.stdout_subscribers.append(q)
                return q
            raise KeyError(f"No live {stream_name} stream for shell {shell_id}")

    async def subscribe_output(self, shell_id: str) -> AsyncQueue[str]:
        """Subscribe to live shell output text for a shell."""
        return await self.subscribe_output_stream(shell_id, "stdout")

    async def subscribe_output_bytes_stream(self, shell_id: str, stream_name: str = "stdout") -> AsyncQueue[bytes]:
        """Subscribe to raw live shell output bytes for a specific stream."""
        async with self._get_lock():
            state = self._pty.get(shell_id)
            if state:
                if stream_name != "stdout":
                    raise KeyError(f"No live {stream_name} stream for shell {shell_id}")
                q: AsyncQueue[bytes] = AsyncQueue()
                state.subscribers_bytes.append(q)
                return q
            pipe_state = self._pipes.get(shell_id)
            if pipe_state:
                q = AsyncQueue()
                if stream_name == "stderr":
                    pipe_state.stderr_subscribers_bytes.append(q)
                else:
                    pipe_state.stdout_subscribers_bytes.append(q)
                return q
            raise KeyError(f"No live {stream_name} stream for shell {shell_id}")

    async def subscribe_output_bytes(self, shell_id: str) -> AsyncQueue[bytes]:
        """Subscribe to raw live output bytes for a shell."""
        return await self.subscribe_output_bytes_stream(shell_id, "stdout")

    async def unsubscribe_output_stream(self, shell_id: str, q: AsyncQueue[str], stream_name: str = "stdout") -> None:
        """Unsubscribe from live shell output for a specific stream."""
        async with self._get_lock():
            state = self._pty.get(shell_id)
            if state:
                if stream_name != "stdout":
                    return
                try:
                    state.subscribers.remove(q)
                except ValueError:
                    pass
                return
            pipe_state = self._pipes.get(shell_id)
            if not pipe_state:
                return
            try:
                if stream_name == "stderr":
                    pipe_state.stderr_subscribers.remove(q)
                else:
                    pipe_state.stdout_subscribers.remove(q)
            except ValueError:
                pass

    async def unsubscribe_output(self, shell_id: str, q: AsyncQueue[str]) -> None:
        """Unsubscribe from live shell output."""
        await self.unsubscribe_output_stream(shell_id, q, "stdout")

    async def unsubscribe_output_bytes_stream(
        self,
        shell_id: str,
        q: AsyncQueue[bytes],
        stream_name: str = "stdout",
    ) -> None:
        """Unsubscribe from raw live shell output for a specific stream."""
        async with self._get_lock():
            state = self._pty.get(shell_id)
            if state:
                if stream_name != "stdout":
                    return
                try:
                    state.subscribers_bytes.remove(q)
                except ValueError:
                    pass
                return
            pipe_state = self._pipes.get(shell_id)
            if not pipe_state:
                return
            try:
                if stream_name == "stderr":
                    pipe_state.stderr_subscribers_bytes.remove(q)
                else:
                    pipe_state.stdout_subscribers_bytes.remove(q)
            except ValueError:
                pass

    async def unsubscribe_output_bytes(self, shell_id: str, q: AsyncQueue[bytes]) -> None:
        """Unsubscribe from raw live output."""
        await self.unsubscribe_output_bytes_stream(shell_id, q, "stdout")

    async def write_to_pty(self, shell_id: str, data: str) -> None:
        """Write data to a shell's PTY."""
        async with self._get_lock():
            state = self._pty.get(shell_id)
        if not state:
            raise KeyError(f"No PTY for shell {shell_id}")
        await self._write_live_pty_state(state, data)

    async def write_to_pipe(self, shell_id: str, data: str) -> None:
        """Write data to a shell's live stdin pipe."""
        async with self._get_lock():
            state = self._pipes.get(shell_id)
            if not state:
                raise KeyError(f"No live pipe for shell {shell_id}")
            if not state.stdin_supported:
                raise RuntimeError(f"Pipe stdin unavailable for shell {shell_id}")
            stdin = state.process.stdin

        if stdin is None:
            raise RuntimeError(f"Pipe stdin unavailable for shell {shell_id}")
        if stdin.is_closing():
            raise RuntimeError(f"Pipe stdin is closed for shell {shell_id}")

        encoded = data.encode("utf-8")
        try:
            stdin.write(encoded)
            await stdin.drain()
        except Exception as exc:
            raise RuntimeError(f"Pipe stdin write failed for shell {shell_id}: {exc}") from exc

    async def send_pipe_eof(self, shell_id: str) -> None:
        """Close stdin for a live pipe-backed shell."""
        async with self._get_lock():
            state = self._pipes.get(shell_id)
            if not state:
                raise KeyError(f"No live pipe for shell {shell_id}")
            if not state.stdin_supported:
                raise RuntimeError(f"Pipe stdin unavailable for shell {shell_id}")
            stdin = state.process.stdin

        if stdin is None:
            raise RuntimeError(f"Pipe stdin unavailable for shell {shell_id}")
        if stdin.is_closing():
            raise RuntimeError(f"Pipe stdin is already closed for shell {shell_id}")

        stdin.close()
        try:
            await stdin.wait_closed()
        except Exception:
            pass

    async def write_to_shell(
        self,
        shell_id: str,
        data: str,
        *,
        append_newline: bool = False,
    ) -> JSONMap:
        payload = str(data)
        if append_newline:
            payload = payload + "\n"
        bytes_written = len(payload.encode("utf-8"))

        try:
            async with self._get_lock():
                pty_state = self._pty.get(shell_id)
                pipe_state = self._pipes.get(shell_id)

            if pty_state is not None:
                backend = str(pty_state.backend or BACKEND_PTY)
                await self._write_live_pty_state(pty_state, payload)
            elif pipe_state is not None:
                record = await self.load_shell_record(shell_id)
                backend = self._backend_name(record) if record else BACKEND_PIPE
                await self.write_to_pipe(shell_id, payload)
            else:
                record = await self.get_shell(shell_id)
                if not record:
                    raise KeyError(f"Shell not found: {shell_id}")
                backend = self._backend_name(record)
                if backend in {"pty", "dtach"}:
                    await self.write_to_pty(shell_id, payload)
                elif backend == "pipe":
                    await self.write_to_pipe(shell_id, payload)
                else:
                    raise RuntimeError(f"stdin write is not supported for backend {backend}")
        except KeyError as exc:
            raise RuntimeError(f"Live input unavailable for shell {shell_id}") from exc

        return {
            "shell_id": shell_id,
            "backend": backend,
            "accepted": True,
            "bytes_written": bytes_written,
            "newline_appended": bool(append_newline),
            "eof_sent": False,
        }

    async def send_shell_eof(self, shell_id: str) -> JSONMap:
        record = await self.get_shell(shell_id)
        if not record:
            raise KeyError(f"Shell not found: {shell_id}")

        backend = self._backend_name(record)
        try:
            if backend == "pipe":
                await self.send_pipe_eof(shell_id)
            else:
                raise RuntimeError(f"stdin EOF is not supported for backend {backend}")
        except KeyError as exc:
            raise RuntimeError(f"Live input unavailable for shell {shell_id}") from exc

        return {
            "shell_id": shell_id,
            "backend": backend,
            "accepted": True,
            "bytes_written": 0,
            "newline_appended": False,
            "eof_sent": True,
        }

    async def resize_pty(self, shell_id: str, cols: int, rows: int) -> None:
        """Resize a shell's PTY."""
        proxy_pid: int | None = None
        async with self._get_lock():
            state = self._pty.get(shell_id)
            if not state:
                raise KeyError(f"No PTY for shell {shell_id}")
            proxy_pid = state.proxy_pid
            winsz = struct.pack("HHHH", max(1, rows), max(1, cols), 0, 0)
            try:
                _ = await asyncio.to_thread(fcntl.ioctl, state.master_fd, termios.TIOCSWINSZ, winsz)
            except Exception:
                pass

        if self._signal_winch_on_resize:
            await self._signal_shell_resize(shell_id, proxy_pid=proxy_pid)

    async def _signal_shell_resize(self, shell_id: str, *, proxy_pid: Optional[int] = None) -> None:
        """Best-effort SIGWINCH delivery after resize_pty().

        Why: interactive programs (readline, shells, TUIs) often cache terminal
        width and rely on SIGWINCH to refresh. In dtach mode, the dtach attach
        proxy can be the "front" process that needs the signal, otherwise the
        app may observe wrap/overwrite glitches in the terminal UI.
        """
        def _try_winch(pid: int) -> None:
            try:
                os.killpg(os.getpgid(pid), signal.SIGWINCH)
                return
            except Exception:
                pass
            try:
                os.kill(pid, signal.SIGWINCH)
            except Exception:
                pass

        # Prefer the dtach attach proxy (directly attached to the resized PTY).
        if proxy_pid:
            _try_winch(int(proxy_pid))

        # Also signal the managed shell PID (if available).
        try:
            rec = await self._load_record(shell_id)
        except Exception:
            rec = None
        if rec and rec.pid:
            _try_winch(int(rec.pid))
