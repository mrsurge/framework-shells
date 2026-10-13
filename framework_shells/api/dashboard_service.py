"""Dashboard state/actions shared by HTTP, WebSocket and Socket.IO transports."""
from __future__ import annotations
import asyncio
import os
import signal
from pathlib import Path
from typing import cast
from ..events import EventType, ShellEvent
from ..process_snapshot import ProcessRecord
from ..shared_manager import get_manager
from ..shutdown import ShutdownPolicy, shutdown_snapshot
from ..protocols.fws_ui import (
    DashboardShellPayload, DashboardProcessPayload, FwsNotification, FwsShellEventMethod,
    SHELL_CREATED_NOTIFICATION_METHOD, SHELL_SPAWNED_NOTIFICATION_METHOD,
    SHELL_UPDATED_NOTIFICATION_METHOD, SHELL_EXITED_NOTIFICATION_METHOD,
    build_shell_event_notification, build_shell_removed_notification,
)

def dashboard_shell_payload(value: object) -> DashboardShellPayload:
    return cast(DashboardShellPayload, value)


def process_payload(proc: ProcessRecord) -> DashboardProcessPayload:
    return {
        "pid": int(proc.pid),
        "parent_pid": int(proc.parent_pid) if proc.parent_pid is not None else None,
        "type": str(proc.type),
        "label": proc.label,
        "shell_id": proc.shell_id,
        "metadata": dict(proc.metadata),
    }


async def dashboard_state_parts() -> tuple[list[DashboardShellPayload], list[DashboardProcessPayload]]:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    described: list[DashboardShellPayload] = []
    for rec in shells:
        try:
            described.append(dashboard_shell_payload(await mgr.describe(rec)))
        except Exception:
            described.append(dashboard_shell_payload(rec.to_payload()))
    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    processes = [process_payload(proc) for proc in snapshot.processes.values()]
    return described, processes


async def dashboard_shell_payload_from_event(event: ShellEvent) -> DashboardShellPayload | None:
    mgr = await get_manager()
    rec = await mgr.load_shell_record(event.shell_id)
    if rec is not None:
        try:
            return dashboard_shell_payload(await mgr.describe(rec))
        except Exception:
            return dashboard_shell_payload(rec.to_payload())
    if event.data:
        return dashboard_shell_payload(dict(event.data))
    return None


async def dashboard_notification_for_event(event: ShellEvent) -> FwsNotification | None:
    if event.type == EventType.SHELL_REMOVED:
        return build_shell_removed_notification(event.shell_id)

    method_by_type: dict[EventType, FwsShellEventMethod] = {
        EventType.SHELL_CREATED: SHELL_CREATED_NOTIFICATION_METHOD,
        EventType.SHELL_SPAWNED: SHELL_SPAWNED_NOTIFICATION_METHOD,
        EventType.SHELL_UPDATED: SHELL_UPDATED_NOTIFICATION_METHOD,
        EventType.SHELL_EXITED: SHELL_EXITED_NOTIFICATION_METHOD,
    }
    method = method_by_type.get(event.type)
    if method is None:
        return None
    shell = await dashboard_shell_payload_from_event(event)
    if shell is None:
        return None
    return build_shell_event_notification(method, shell)


async def truncate_log_file(path: Path, *, logs_root: Path) -> bool:
    resolved = path.resolve(strict=False)
    if resolved.suffix != ".log":
        return False
    if logs_root != resolved and logs_root not in resolved.parents:
        return False
    try:
        if not resolved.exists() or not resolved.is_file():
            return False
        await asyncio.to_thread(lambda: resolved.open("wb").close())
        return True
    except Exception:
        return False


async def action_truncate_logs() -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()

    logs_root = Path(mgr.logs_dir).resolve(strict=False)
    candidates: list[Path] = []
    for rec in shells:
        candidates.append(Path(rec.stdout_log))
        candidates.append(Path(rec.stderr_log))
        if rec.io_metadata_log:
            candidates.append(Path(rec.io_metadata_log))

    try:
        candidates.extend(list(Path(mgr.logs_dir).glob("*.log")))
    except Exception:
        pass

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        _ = await truncate_log_file(resolved, logs_root=logs_root)

    for rec in shells:
        await mgr.emit_log_reset(rec.id, "stdout")
        await mgr.emit_log_reset(rec.id, "stderr")


async def action_purge_exited() -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    exited = [s for s in shells if (getattr(s, "status", None) or "") == "exited"]
    for rec in exited:
        try:
            _ = await mgr.remove_shell(rec.id, force=True)
        except Exception:
            pass


async def action_terminate_shell(shell_id: str) -> None:
    mgr = await get_manager()
    await mgr.terminate_shell(shell_id, force=True)


async def action_shell_input(
    shell_id: str,
    data: str | None,
    *,
    append_newline: bool = False,
    eof: bool = False,
    source: str = "dashboard",
) -> None:
    mgr = await get_manager()
    if eof:
        _ = await mgr.send_shell_eof(shell_id, source=source)
        return
    if data is None:
        raise ValueError("data is required unless eof=true")
    _ = await mgr.write_to_shell(shell_id, data, append_newline=append_newline, source=source)


async def action_purge_shell(shell_id: str) -> None:
    mgr = await get_manager()
    _ = await mgr.remove_shell(shell_id, force=True)


async def action_terminate_pid(pid: int) -> None:
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        pass


async def action_shutdown_app(app_id: str) -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    targets = [s for s in shells if (s.derive_app_id() or "") == app_id and s.pid and s.status == "running"]
    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    root_pids = [s.pid for s in targets if s.pid]
    _ = await shutdown_snapshot(snapshot, manager=mgr, policy=ShutdownPolicy(types_last=[]), root_pids=root_pids)


async def action_shutdown(scope: str) -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()

    if scope == "shells":
        for s in shells:
            if s.pid and s.status == "running":
                await mgr.terminate_shell(s.id, force=True)
        return

    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    _ = await shutdown_snapshot(snapshot, manager=mgr, policy=ShutdownPolicy(types_last=[]))
