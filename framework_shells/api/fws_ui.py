from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import re
import signal
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Annotated, cast

import aiofiles
import fnmatch
from fastapi import APIRouter, Form, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from .. import get_manager
from ..events import EventType, ShellEvent, get_event_bus
from ..process_snapshot import ProcessRecord
from ..protocols.jsonrpc import dump_json_line
from ..protocols.fws_ui import (
    APP_SHUTDOWN_METHOD,
    DASHBOARD_OPEN_METHOD,
    DASHBOARD_REFRESH_METHOD,
    FwsShellEventMethod,
    SHELL_CREATED_NOTIFICATION_METHOD,
    SHELL_EXITED_NOTIFICATION_METHOD,
    SHELL_SPAWNED_NOTIFICATION_METHOD,
    SHELL_UPDATED_NOTIFICATION_METHOD,
    DashboardProcessPayload,
    DashboardShellPayload,
    EXITED_PURGE_METHOD,
    FwsNotification,
    FwsRequest,
    PidTerminateRequest,
    ShellPurgeRequest,
    ShellTerminateRequest,
    ShutdownRequest,
    AppShutdownRequest,
    build_dashboard_open_response,
    build_dashboard_refresh_response,
    build_action_response,
    build_logs_chunk_notification,
    build_logs_initial_notification,
    build_logs_open_response,
    build_logs_reset_notification,
    build_shell_event_notification,
    build_shell_removed_notification,
    build_request_error_response,
    build_error_notification,
    parse_fws_request,
    LOGS_OPEN_METHOD,
    LOGS_TRUNCATE_METHOD,
    PID_TERMINATE_METHOD,
    SHELL_PURGE_METHOD,
    SHELL_TERMINATE_METHOD,
    SHUTDOWN_METHOD,
    LogStreamName,
)
from ..shutdown import ShutdownPolicy, shutdown_snapshot


router = APIRouter()

_UI_DIR = Path(__file__).resolve().parent.parent / "ui"
ShellInfo = dict[str, object]
StyleMap = dict[str, dict[str, str]]


def _dashboard_shell_payload(value: object) -> DashboardShellPayload:
    return cast(DashboardShellPayload, value)


def _process_payload(proc: ProcessRecord) -> DashboardProcessPayload:
    return {
        "pid": int(proc.pid),
        "parent_pid": int(proc.parent_pid) if proc.parent_pid is not None else None,
        "type": str(proc.type),
        "label": proc.label,
        "shell_id": proc.shell_id,
        "metadata": dict(proc.metadata),
    }


async def _dashboard_state_parts() -> tuple[list[DashboardShellPayload], list[DashboardProcessPayload]]:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    described: list[DashboardShellPayload] = []
    for rec in shells:
        try:
            described.append(_dashboard_shell_payload(await mgr.describe(rec)))
        except Exception:
            described.append(_dashboard_shell_payload(rec.to_payload()))
    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    processes = [_process_payload(proc) for proc in snapshot.processes.values()]
    return described, processes


async def _dashboard_shell_payload_from_event(event: ShellEvent) -> DashboardShellPayload | None:
    mgr = await get_manager()
    rec = await mgr.load_shell_record(event.shell_id)
    if rec is not None:
        try:
            return _dashboard_shell_payload(await mgr.describe(rec))
        except Exception:
            return _dashboard_shell_payload(rec.to_payload())
    if event.data:
        return _dashboard_shell_payload(dict(event.data))
    return None


async def _dashboard_notification_for_event(event: ShellEvent) -> FwsNotification | None:
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
    shell = await _dashboard_shell_payload_from_event(event)
    if shell is None:
        return None
    return build_shell_event_notification(method, shell)


async def _send_ws_payload(websocket: WebSocket, payload: Mapping[str, object]) -> None:
    await websocket.send_text(dump_json_line(payload))


async def _send_ws_notification(websocket: WebSocket, notification: FwsNotification) -> None:
    await _send_ws_payload(websocket, notification)


async def _send_ws_response(websocket: WebSocket, response: Mapping[str, object]) -> None:
    await _send_ws_payload(websocket, response)


async def _send_ws_error_response(
    websocket: WebSocket,
    request_id: str | None,
    *,
    code: int,
    message: str,
    error_code: str | None = None,
    shell_id: str | None = None,
) -> None:
    await _send_ws_response(
        websocket,
        build_request_error_response(
            request_id,
            code=code,
            message=message,
            error_code=error_code,
            shell_id=shell_id,
        ),
    )


async def _receive_ws_text(
    websocket: WebSocket,
    *,
    timeout_seconds: float = 5.0,
) -> str | None:
    try:
        return await asyncio.wait_for(websocket.receive_text(), timeout=timeout_seconds)
    except Exception:
        return None


def _escape_html(value: object | None) -> str:
    s = "" if value is None else str(value)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

def _fmt_bytes(n: object) -> str:
    if isinstance(n, bool):
        return "-"
    if isinstance(n, int):
        val = n
    elif isinstance(n, float):
        val = int(n)
    elif isinstance(n, str):
        try:
            val = int(n)
        except ValueError:
            return "-"
    else:
        return "-"
    if val <= 0:
        return "0"
    mib = val / (1024 * 1024)
    if mib >= 1024:
        gib = mib / 1024
        return f"{gib:.1f} GiB"
    return f"{mib:.0f} MiB"

def _fmt_cpu(pct: object) -> str:
    if isinstance(pct, bool):
        return "-"
    if isinstance(pct, (int, float)):
        val = float(pct)
    elif isinstance(pct, str):
        try:
            val = float(pct)
        except ValueError:
            return "-"
    else:
        return "-"
    if val < 0:
        return "-"
    return f"{val:.1f}%"


def _as_dict(value: object) -> ShellInfo:
    if isinstance(value, dict):
        return cast(ShellInfo, value)
    return {}


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return cast(list[object], value)
    return []


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _float_or_zero(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _shell_backend(info: ShellInfo) -> str:
    backend = ""
    if info.get("backend"):
        backend = str(info.get("backend"))
    elif info.get("uses_dtach"):
        backend = "dtach"
    elif info.get("uses_pipes"):
        backend = "pipes"
    elif info.get("uses_pty"):
        backend = "pty"
    else:
        backend = "proc"

    pipe_runtime = _as_dict(info.get("pipe_runtime"))
    if backend == "pipe" and pipe_runtime.get("engine") == "native-pipe":
        return "pipe:native-pipe"
    if backend == "pipe" and pipe_runtime.get("engine") == "native-terminal-pipe":
        return "pipe:native-terminal-pipe"
    if backend == "pipe" and pipe_runtime.get("engine") == "python-terminal-pipe":
        return "pipe:python-terminal-pipe"

    return backend


def _is_shell_live(info: ShellInfo) -> bool:
    if not info:
        return False
    if info.get("status") != "running":
        return False
    if not info.get("pid"):
        return False
    stats = _as_dict(info.get("stats"))
    if stats and stats.get("alive") is False:
        return False
    return True


_CSS_COLOR_RE = re.compile(r"^[#()0-9a-zA-Z.,%\s-]+$")


def _safe_css_value(value: object) -> str:
    s = "" if value is None else str(value).strip()
    if not s:
        return ""
    if not _CSS_COLOR_RE.match(s):
        return ""
    return s


def _collect_subgroup_styles(shells: list[ShellInfo]) -> StyleMap:
    merged: StyleMap = {}
    for info in shells:
        ui = _as_dict(info.get("ui"))
        if not ui:
            continue
        raw = ui.get("subgroup_styles") or ui.get("subgroupStyles")
        raw_styles = _as_dict(raw)
        if not raw_styles:
            continue
        for key, style_value in raw_styles.items():
            style = _as_dict(style_value)
            if not style:
                continue
            bg = _safe_css_value(style.get("bg") or style.get("background"))
            border = _safe_css_value(style.get("border") or style.get("border_color") or style.get("borderColor"))
            color = _safe_css_value(style.get("color") or style.get("fg") or style.get("foreground"))
            normalized: dict[str, str] = {}
            if bg:
                normalized["bg"] = bg
            if border:
                normalized["border"] = border
            if color:
                normalized["color"] = color
            if normalized:
                merged[str(key)] = normalized
    return merged


def _subgroup_style_for(name: str, styles: StyleMap) -> dict[str, str]:
    if not name:
        return {}
    if name in styles:
        return styles.get(name, {})
    best_key: str | None = None
    for pattern in styles.keys():
        if pattern == name:
            best_key = pattern
            break
        if any(ch in pattern for ch in "*?[]") and fnmatch.fnmatchcase(name, pattern):
            if best_key is None or len(pattern) > len(best_key):
                best_key = pattern
    if best_key is None:
        return {}
    return styles.get(best_key, {})


def _card_style_for_subgroups(subgroups: list[str], styles: StyleMap) -> dict[str, str]:
    if not subgroups:
        return {}
    preferred = list(subgroups[1:]) + list(subgroups[:1])
    for subgroup in preferred:
        style = _subgroup_style_for(str(subgroup), styles)
        if style:
            return style
    return {}


def _render_subgroup_pills(subgroups: list[object], styles: StyleMap) -> str:
    pills: list[str] = []
    for raw in subgroups:
        name = str(raw or "").strip()
        if not name:
            continue
        style = _subgroup_style_for(name, styles)
        css_bits: list[str] = []
        if style.get("bg"):
            css_bits.append(f"background: {style['bg']};")
        if style.get("border"):
            css_bits.append(f"border-color: {style['border']};")
        if style.get("color"):
            css_bits.append(f"color: {style['color']};")
        style_attr = f' style="{" ".join(css_bits)}"' if css_bits else ""
        pills.append(f'<span class="pill"{style_attr}>{_escape_html(name)}</span>')
    if not pills:
        return ""
    return '<div class="row">' + "".join(pills) + "</div>"


def _render_copy_field(label: str, value: object | None, *, extra_classes: str = "") -> str:
    raw = "" if value is None else str(value)
    classes = "copy-field"
    if extra_classes:
        classes += f" {extra_classes}"
    return (
        f'<div class="{classes}" data-copy="{_escape_html(raw)}" role="button" tabindex="0">'
        f'<div class="copy-field-label">{_escape_html(label)}</div>'
        f'<div class="copy-field-value">{_escape_html(raw)}</div>'
        '<button class="copy-overlay" type="button" aria-label="Copy field value">Copy</button>'
        "</div>"
    )


def _exited_timestamp(info: ShellInfo) -> float:
    raw = info.get("updated_at")
    if raw is None:
        raw = info.get("created_at")
    if raw is None:
        return 0.0
    return _float_or_zero(raw)


def _fmt_exited_timestamp(ts: float) -> str:
    if ts <= 0:
        return "Unknown time"
    dt = datetime.fromtimestamp(ts)
    now = datetime.now()
    if dt.date() == now.date():
        return dt.strftime("%H:%M")
    return dt.strftime("%m/%d/%Y %H:%M")


def _exited_token(exited: list[ShellInfo]) -> str:
    digest = hashlib.sha1()
    for item in sorted(exited, key=_exited_timestamp, reverse=True):
        digest.update(str(item.get("id") or "").encode("utf-8", errors="replace"))
        digest.update(b"|")
        digest.update(str(item.get("updated_at") or item.get("created_at") or "").encode("utf-8", errors="replace"))
        digest.update(b";")
    return digest.hexdigest()


def _render_exited_content(exited: list[ShellInfo], subgroup_styles: StyleMap) -> str:
    parts: list[str] = []
    if not exited:
        parts.append('<div class="shell-card"><div class="shell-meta">No exited shells.</div></div>')
        return "\n".join(parts)

    for s in sorted(exited, key=_exited_timestamp, reverse=True):
        sid = str(s.get("id") or "")
        label = str(s.get("label") or sid)
        status = str(s.get("status") or "exited")
        exit_code = s.get("exit_code")
        exited_ts = _exited_timestamp(s)
        exited_stamp = _fmt_exited_timestamp(exited_ts)
        subgroups = _as_list(s.get("subgroups"))
        style = _card_style_for_subgroups([str(x) for x in subgroups], subgroup_styles)
        style_bits: list[str] = []
        if style.get("bg"):
            style_bits.append(f"background: {style['bg']};")
        if style.get("border"):
            style_bits.append(f"border-color: {style['border']}; border-left: 4px solid {style['border']};")
        style_attr = f' style="{" ".join(style_bits)}"' if style_bits else ""
        meta = status
        if exit_code is not None:
            meta += f" · exit: {exit_code}"
        cmd = _as_list(s.get("command"))
        command_text = " ".join(map(str, cmd))
        stdout_log = str(s.get("stdout_log") or "")
        stderr_log = str(s.get("stderr_log") or "")
        logs_available = Path(stdout_log).exists() or Path(stderr_log).exists()

        parts.append(f'<div class="exited-item" data-exited-item="1" data-exited-ts="{_escape_html(exited_ts)}">')
        parts.append('<div class="exited-ts">%s</div>' % _escape_html(exited_stamp))
        parts.append(f'<div class="shell-card shell-entry is-collapsed"{style_attr} data-shell-id="{_escape_html(sid)}">')
        parts.append('<div class="shell-header">')
        parts.append('<div class="shell-title">%s</div>' % _escape_html(label))
        parts.append('<div class="shell-actions">')
        parts.append(f'<button class="btn btn-small" type="button" data-collapse-toggle="{_escape_html(sid)}" aria-expanded="false">Expand</button>')
        if logs_available:
            parts.append(
                f'<button class="btn btn-small" type="button" data-log-open="{_escape_html(sid)}" data-log-label="{_escape_html(label)}">Logs</button>'
            )
        else:
            parts.append('<button class="btn btn-small" type="button" disabled>Logs Purged</button>')
        parts.append(
            f'<form method="post" action="/fws/action/shell/{_escape_html(sid)}/purge" data-fws-ajax="1"><button class="btn btn-small" type="submit">Purge</button></form>'
        )
        parts.append("</div>")
        parts.append("</div>")
        parts.append(f'<div class="shell-details" data-collapse-content="{_escape_html(sid)}">')
        parts.append(_render_copy_field("Status", meta))
        parts.append(_render_copy_field("ID", sid))
        parts.append(_render_copy_field("Command", command_text, extra_classes="copy-field--multiline"))
        parts.append(_render_copy_field("stdout log", stdout_log, extra_classes="copy-field--path"))
        parts.append(_render_copy_field("stderr log", stderr_log, extra_classes="copy-field--path"))
        pills = _render_subgroup_pills(subgroups, subgroup_styles)
        if pills:
            parts.append(pills)
        parts.append("</div>")
        parts.append("</div>")
        parts.append("</div>")
    if len(exited) > 50:
        parts.append('<div class="row exited-more-row">')
        parts.append('<button class="btn btn-small" type="button" id="fws-exited-more">More</button>')
        parts.append("</div>")
    return "\n".join(parts)


async def _render_dashboard_html() -> str:  # pyright: ignore[reportUnusedFunction]
    mgr = await get_manager()
    shells = await mgr.list_shells()
    described: list[ShellInfo] = []
    for rec in shells:
        try:
            described.append(await mgr.describe(rec))
        except Exception:
            described.append(rec.to_payload())

    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)

    shell_pid_set = {info.get("pid") for info in described if info.get("pid")}
    children_by_parent: dict[int, list[ProcessRecord]] = {}
    for proc in snapshot.processes.values():
        if proc.parent_pid is None:
            continue
        try:
            children_by_parent.setdefault(int(proc.parent_pid), []).append(proc)
        except Exception:
            continue

    running = [s for s in described if _is_shell_live(s)]
    exited = [s for s in described if not _is_shell_live(s)]
    exited_token = _exited_token(exited)
    subgroup_styles = _collect_subgroup_styles(described)

    parts: list[str] = []

    parts.append('<div class="section">')
    parts.append('<div class="section-title">Running <span class="muted">(%d)</span></div>' % len(running))

    if not running:
        parts.append('<div class="shell-card"><div class="shell-meta">No running shells.</div></div>')
    else:
        groups: dict[str, dict[str, list[ShellInfo]]] = {}
        for s in running:
            subgroups = _as_list(s.get("subgroups"))
            normalized = [str(x) for x in subgroups if str(x).strip()]
            umbrella = normalized[0] if len(normalized) >= 1 else "(ungrouped)"
            subgroup = normalized[1] if len(normalized) >= 2 else "(root)"
            groups.setdefault(umbrella, {}).setdefault(subgroup, []).append(s)

        def _group_sort_key(name: str) -> tuple[int, str]:
            return (1, "") if name == "(ungrouped)" else (0, name.lower())

        def _subgroup_sort_key(name: str) -> tuple[int, str]:
            return (0, "") if name == "app-worker" else (1, name.lower())

        def _shell_sort_key(info: ShellInfo) -> tuple[int, str, str]:
            label = str(info.get("label") or "")
            return (0 if label.startswith("app-worker:") else 1, label.lower(), str(info.get("id") or ""))

        for umbrella in sorted(groups.keys(), key=_group_sort_key):
            subgroup_map = groups.get(umbrella, {})
            total_shells = sum(len(v) for v in subgroup_map.values())

            group_id = str(umbrella)
            parts.append(f'<div class="group-card is-collapsed" data-group-id="{_escape_html(group_id)}">')
            parts.append('<div class="group-header">')
            parts.append('<div class="group-title">%s</div>' % _escape_html(umbrella))
            parts.append('<div class="shell-actions">')
            parts.append(
                f'<button class="btn btn-small" type="button" data-group-toggle="{_escape_html(group_id)}" aria-expanded="false">Expand</button>'
            )
            if umbrella != "(ungrouped)":
                parts.append(
                    f'<form method="post" action="/fws/action/app/{_escape_html(umbrella)}/shutdown" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Shutdown Group</button></form>'
                )
            parts.append("</div>")
            parts.append("</div>")
            parts.append(
                '<div class="group-meta">Shells: %s · Subgroups: %s</div>'
                % (_escape_html(total_shells), _escape_html(len(subgroup_map)))
            )

            parts.append(f'<div class="group-content" data-group-content="{_escape_html(group_id)}">')
            for subgroup in sorted(subgroup_map.keys(), key=_subgroup_sort_key):
                style = _subgroup_style_for(subgroup, subgroup_styles)
                style_bits: list[str] = []
                if style.get("bg"):
                    style_bits.append(f"background: {style['bg']};")
                if style.get("border"):
                    style_bits.append(f"border-color: {style['border']}; border-left: 4px solid {style['border']};")
                style_attr = f' style="{" ".join(style_bits)}"' if style_bits else ""

                shells_in_group = sorted(subgroup_map.get(subgroup, []), key=_shell_sort_key)
                parts.append(f'<div class="subgroup-card"{style_attr}>')
                parts.append('<div class="subgroup-header">')
                parts.append('<div class="subgroup-title">%s</div>' % _escape_html(subgroup))
                parts.append('<div class="subgroup-count muted">(%d)</div>' % len(shells_in_group))
                parts.append("</div>")

                for s in shells_in_group:
                    sid = str(s.get("id") or "")
                    label = str(s.get("label") or sid)
                    pid = s.get("pid")
                    backend = _shell_backend(s)
                    subgroups = _as_list(s.get("subgroups"))
                    stats = _as_dict(s.get("stats"))
                    cpu = _fmt_cpu(stats.get("cpu_percent"))
                    rss = _fmt_bytes(stats.get("memory_rss"))

                    row_style = _card_style_for_subgroups([str(x) for x in subgroups], subgroup_styles)
                    row_style_bits: list[str] = []
                    if row_style.get("bg"):
                        row_style_bits.append(f"background: {row_style['bg']};")
                    if row_style.get("border"):
                        row_style_bits.append(f"border-left: 3px solid {row_style['border']};")
                    row_style_attr = f' style="{" ".join(row_style_bits)}"' if row_style_bits else ""
                    status = str(s.get("status") or "running")
                    cmd = _as_list(s.get("command"))
                    command_text = " ".join(map(str, cmd))
                    stdout_log = str(s.get("stdout_log") or "")
                    stderr_log = str(s.get("stderr_log") or "")

                    parts.append(f'<div class="shell-card shell-entry is-collapsed"{row_style_attr} data-shell-id="{_escape_html(sid)}">')
                    parts.append('<div class="shell-header">')
                    parts.append('<div class="shell-title">%s</div>' % _escape_html(label))
                    parts.append('<div class="shell-actions">')
                    parts.append(f'<button class="btn btn-small" type="button" data-collapse-toggle="{_escape_html(sid)}" aria-expanded="false">Expand</button>')
                    parts.append(
                        f'<button class="btn btn-small" type="button" data-log-open="{_escape_html(sid)}" data-log-label="{_escape_html(label)}">Logs</button>'
                    )
                    parts.append(
                        f'<form method="post" action="/fws/action/shell/{_escape_html(sid)}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Stop</button></form>'
                    )
                    parts.append("</div>")
                    parts.append("</div>")
                    parts.append(f'<div class="shell-details" data-collapse-content="{_escape_html(sid)}">')
                    parts.append(_render_copy_field("Status", status))
                    parts.append(_render_copy_field("PID", pid))
                    parts.append(_render_copy_field("ID", sid))
                    parts.append(_render_copy_field("Backend", backend))
                    parts.append(_render_copy_field("CPU", cpu))
                    parts.append(_render_copy_field("RSS", rss))
                    parts.append(_render_copy_field("Command", command_text, extra_classes="copy-field--multiline"))
                    parts.append(_render_copy_field("stdout log", stdout_log, extra_classes="copy-field--path"))
                    parts.append(_render_copy_field("stderr log", stderr_log, extra_classes="copy-field--path"))
                    pills = _render_subgroup_pills(subgroups, subgroup_styles)
                    if pills:
                        parts.append(pills)

                    # Hard tree children (pid parent/child).
                    pid_int = _int_or_none(pid)
                    if pid_int is not None and pid_int in children_by_parent:
                        children = [p for p in children_by_parent.get(pid_int, []) if p.pid not in shell_pid_set]
                        if children:
                            parts.append('<div class="children">')
                            parts.append('<div class="children-title">Child Processes (%d)</div>' % len(children))
                            for child in sorted(children, key=lambda p: (p.type, p.pid)):
                                parts.append('<div class="child-row child-row--proc">')
                                parts.append('<div class="child-main">')
                                parts.append('<div class="child-label">%s</div>' % _escape_html(child.label or child.pid))
                                parts.append('<div class="child-meta-line">')
                                parts.append(
                                    '<div class="child-meta">PID: %s · %s</div>'
                                    % (_escape_html(child.pid), _escape_html(child.type))
                                )
                                parts.append('<div class="row child-actions-inline">')
                                parts.append(
                                    f'<form method="post" action="/fws/action/pid/{_escape_html(child.pid)}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Kill</button></form>'
                                )
                                parts.append("</div>")
                                parts.append("</div>")
                                parts.append("</div>")
                                parts.append("</div>")
                            parts.append("</div>")
                    parts.append("</div>")
                    parts.append("</div>")

                parts.append("</div>")
            parts.append("</div>")

            parts.append("</div>")

    parts.append("</div>")

    parts.append('<div class="section section-exited" id="fws-exited">')
    parts.append('<div class="section-title">')
    parts.append('Exited <span class="muted">(%d)</span>' % len(exited))
    parts.append('<div class="shell-actions">')
    parts.append('<button class="btn btn-small" type="button" id="fws-exited-toggle" aria-expanded="false">Expand Exited</button>')
    if exited:
        parts.append(
            '<form method="post" action="/fws/action/exited/purge" data-fws-ajax="1" data-confirm="Purge ALL exited shells (delete their logs + metadata)?"><button class="btn btn-small btn-danger" type="submit">Purge Exited</button></form>'
        )
    parts.append("</div>")
    parts.append("</div>")
    parts.append(
        f'<div class="exited-content is-collapsed" id="fws-exited-content" data-loaded="0" data-count="{_escape_html(len(exited))}" data-token="{_escape_html(exited_token)}"></div>'
    )
    parts.append("</div>")
    parts.append("</div>")

    return "\n".join(parts)


@router.get("/fws")
async def fws_root() -> RedirectResponse:
    return RedirectResponse(url="/fws/", status_code=308)


@router.get("/fws/")
async def fws_index() -> FileResponse:
    return FileResponse(
        _UI_DIR / "index.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.get("/fws/exited", response_class=HTMLResponse)
async def fws_exited_fragment() -> HTMLResponse:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    described: list[ShellInfo] = []
    for rec in shells:
        try:
            described.append(await mgr.describe(rec))
        except Exception:
            described.append(rec.to_payload())
    subgroup_styles = _collect_subgroup_styles(described)
    exited = [s for s in described if not _is_shell_live(s)]
    return HTMLResponse(content=_render_exited_content(exited, subgroup_styles))


@router.get("/fws/static/{path:path}")
async def fws_static(path: str) -> FileResponse:
    target = (_UI_DIR / path).resolve()
    if not target.is_file() or _UI_DIR not in target.parents:
        raise HTTPException(status_code=404, detail="Not found")
    media_type, _ = mimetypes.guess_type(str(target))
    return FileResponse(
        target,
        media_type=media_type or "application/octet-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.post("/fws/action/refresh")
async def fws_refresh() -> RedirectResponse:
    return RedirectResponse(url="/fws/", status_code=303)


def _is_ajax(request: Request) -> bool:
    return (request.headers.get("x-fws-ajax") or "").strip() == "1"


async def _truncate_log_file(path: Path, *, logs_root: Path) -> bool:
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


async def _action_truncate_logs() -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()

    logs_root = Path(mgr.logs_dir).resolve(strict=False)
    candidates: list[Path] = []
    for rec in shells:
        candidates.append(Path(rec.stdout_log))
        candidates.append(Path(rec.stderr_log))

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
        _ = await _truncate_log_file(resolved, logs_root=logs_root)

    for rec in shells:
        await mgr.emit_log_reset(rec.id, "stdout")
        await mgr.emit_log_reset(rec.id, "stderr")


async def _action_purge_exited() -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    exited = [s for s in shells if (getattr(s, "status", None) or "") == "exited"]
    for rec in exited:
        try:
            _ = await mgr.remove_shell(rec.id, force=True)
        except Exception:
            pass


async def _action_terminate_shell(shell_id: str) -> None:
    mgr = await get_manager()
    await mgr.terminate_shell(shell_id, force=True)


async def _action_purge_shell(shell_id: str) -> None:
    mgr = await get_manager()
    _ = await mgr.remove_shell(shell_id, force=True)


async def _action_terminate_pid(pid: int) -> None:
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        pass


async def _action_shutdown_app(app_id: str) -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()
    targets = [s for s in shells if (s.derive_app_id() or "") == app_id and s.pid and s.status == "running"]
    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    root_pids = [s.pid for s in targets if s.pid]
    _ = await shutdown_snapshot(snapshot, manager=mgr, policy=ShutdownPolicy(types_last=[]), root_pids=root_pids)


async def _action_shutdown(scope: str) -> None:
    mgr = await get_manager()
    shells = await mgr.list_shells()

    if scope == "shells":
        for s in shells:
            if s.pid and s.status == "running":
                await mgr.terminate_shell(s.id, force=True)
        return

    snapshot = await mgr.build_process_snapshot(shells=shells, include_procfs_descendants=True)
    _ = await shutdown_snapshot(snapshot, manager=mgr, policy=ShutdownPolicy(types_last=[]))


@router.post("/fws/action/logs/purge")
async def fws_purge_logs(request: Request) -> Response:
    await _action_truncate_logs()

    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/exited/purge")
async def fws_purge_exited(request: Request) -> Response:
    await _action_purge_exited()
    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/shell/{shell_id}/terminate")
async def fws_terminate_shell(shell_id: str, request: Request) -> Response:
    await _action_terminate_shell(shell_id)
    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/shell/{shell_id}/purge")
async def fws_purge_shell(shell_id: str, request: Request) -> Response:
    await _action_purge_shell(shell_id)
    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/pid/{pid}/terminate")
async def fws_terminate_pid(pid: int, request: Request) -> Response:
    await _action_terminate_pid(pid)
    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/app/{app_id}/shutdown")
async def fws_shutdown_app(app_id: str, request: Request) -> Response:
    await _action_shutdown_app(app_id)
    if _is_ajax(request):
        return Response(status_code=204)
    return RedirectResponse(url="/fws/", status_code=303)


@router.post("/fws/action/shutdown")
async def fws_shutdown(scope: Annotated[str, Form()] = "tree") -> RedirectResponse:
    await _action_shutdown(scope)
    return RedirectResponse(url="/fws/", status_code=303)


@router.websocket("/ws/fws")
async def fws_ws(websocket: WebSocket):
    await websocket.accept()
    open_raw = await _receive_ws_text(websocket)
    open_request = parse_fws_request(open_raw) if open_raw is not None else None
    if open_request is None or open_request["method"] != DASHBOARD_OPEN_METHOD:
        await _send_ws_error_response(
            websocket,
            open_request["id"] if open_request is not None else None,
            code=-32600,
            message=f"Expected {DASHBOARD_OPEN_METHOD} request",
            error_code="invalid_open",
        )
        try:
            await websocket.close()
        except Exception:
            pass
        return

    bus = get_event_bus()
    q = bus.subscribe()

    async def handle_request(request: FwsRequest) -> None:
        method = request["method"]
        request_id = request["id"]
        try:
            if method == DASHBOARD_OPEN_METHOD:
                shells, processes = await _dashboard_state_parts()
                await _send_ws_response(websocket, build_dashboard_open_response(request_id, shells, processes))
                return
            if method == DASHBOARD_REFRESH_METHOD:
                shells, processes = await _dashboard_state_parts()
                await _send_ws_response(websocket, build_dashboard_refresh_response(request_id, shells, processes))
                return
            if method == LOGS_TRUNCATE_METHOD:
                await _action_truncate_logs()
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == EXITED_PURGE_METHOD:
                await _action_purge_exited()
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == SHELL_TERMINATE_METHOD:
                shell_request = cast(ShellTerminateRequest, request)
                await _action_terminate_shell(shell_request["params"]["shell_id"])
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == SHELL_PURGE_METHOD:
                shell_request = cast(ShellPurgeRequest, request)
                await _action_purge_shell(shell_request["params"]["shell_id"])
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == PID_TERMINATE_METHOD:
                pid_request = cast(PidTerminateRequest, request)
                await _action_terminate_pid(pid_request["params"]["pid"])
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == APP_SHUTDOWN_METHOD:
                app_request = cast(AppShutdownRequest, request)
                await _action_shutdown_app(app_request["params"]["app_id"])
                await _send_ws_response(websocket, build_action_response(request_id))
                return
            if method == SHUTDOWN_METHOD:
                shutdown_request = cast(ShutdownRequest, request)
                await _action_shutdown(shutdown_request["params"]["scope"])
                await _send_ws_response(websocket, build_action_response(request_id))
                return

            await _send_ws_error_response(
                websocket,
                request_id,
                code=-32601,
                message=f"Method not found: {method}",
                error_code="method_not_found",
            )
        except Exception as exc:
            await _send_ws_error_response(
                websocket,
                request_id,
                code=-32000,
                message=str(exc),
                error_code="action_failed",
            )

    try:
        await handle_request(open_request)
        receive_task = asyncio.create_task(websocket.receive_text())
        bus_task = asyncio.create_task(q.get())
        while True:
            done, _ = await asyncio.wait({receive_task, bus_task}, return_when=asyncio.FIRST_COMPLETED)

            if receive_task in done:
                try:
                    raw = receive_task.result()
                except WebSocketDisconnect:
                    break
                except Exception:
                    break

                request = parse_fws_request(raw)
                if request is None:
                    await _send_ws_error_response(
                        websocket,
                        None,
                        code=-32600,
                        message="Invalid request",
                        error_code="invalid_request",
                    )
                else:
                    await handle_request(request)
                receive_task = asyncio.create_task(websocket.receive_text())

            if bus_task in done:
                try:
                    event = bus_task.result()
                except Exception:
                    break
                notification = await _dashboard_notification_for_event(event)
                if notification is not None:
                    await _send_ws_notification(websocket, notification)
                bus_task = asyncio.create_task(q.get())
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        for task_name in ("receive_task", "bus_task"):
            task = locals().get(task_name)
            if isinstance(task, asyncio.Task):
                _ = task.cancel()
        try:
            bus.unsubscribe(q)
        except Exception:
            pass


@router.get("/fws/logs/{shell_id}", response_class=HTMLResponse)
async def fws_logs(shell_id: str):
    return RedirectResponse(url=f"/fws/?log={shell_id}", status_code=307)


@router.websocket("/ws/fws/logs/{shell_id}")
async def fws_logs_ws(websocket: WebSocket, shell_id: str):
    await websocket.accept()

    async def safe_close() -> None:
        try:
            await websocket.close()
        except Exception:
            pass

    open_raw = await _receive_ws_text(websocket)
    open_request = parse_fws_request(open_raw) if open_raw is not None else None
    if open_request is None or open_request["method"] != LOGS_OPEN_METHOD:
        await _send_ws_error_response(
            websocket,
            open_request["id"] if open_request is not None else None,
            code=-32600,
            message=f"Expected {LOGS_OPEN_METHOD} request for {shell_id}",
            error_code="invalid_open",
            shell_id=shell_id,
        )
        await safe_close()
        return

    logs_open_request = open_request

    if logs_open_request["params"]["shell_id"] != shell_id:
        await _send_ws_error_response(
            websocket,
            logs_open_request["id"],
            code=-32602,
            message=f"shell_id mismatch for {shell_id}",
            error_code="shell_id_mismatch",
            shell_id=shell_id,
        )
        await safe_close()
        return

    try:
        mgr = await get_manager()
        rec = await mgr.load_shell_record(shell_id)
    except Exception as exc:
        await _send_ws_error_response(
            websocket,
            logs_open_request["id"],
            code=-32000,
            message=f"Failed to load shell record: {exc}",
            error_code="shell_lookup_failed",
            shell_id=shell_id,
        )
        await safe_close()
        return

    if not rec:
        await _send_ws_error_response(
            websocket,
            logs_open_request["id"],
            code=-32004,
            message=f"Shell not found: {shell_id}",
            error_code="shell_not_found",
            shell_id=shell_id,
        )
        await safe_close()
        return

    stdout_path = Path(rec.stdout_log)
    stderr_path = Path(rec.stderr_log)

    bus = get_event_bus()
    q = bus.subscribe()

    try:
        await _send_ws_response(websocket, build_logs_open_response(logs_open_request["id"], shell_id))
        stdout_lines: list[str] = []
        if stdout_path.exists():
            async with aiofiles.open(stdout_path, "r", encoding="utf-8", errors="replace") as f:
                stdout_lines = (await f.read()).splitlines()

        stderr_lines: list[str] = []
        if stderr_path.exists():
            async with aiofiles.open(stderr_path, "r", encoding="utf-8", errors="replace") as f:
                stderr_lines = (await f.read()).splitlines()

        await _send_ws_notification(
            websocket,
            build_logs_initial_notification(
                shell_id,
                "\n".join(stdout_lines[-2000:]),
                "\n".join(stderr_lines[-2000:]),
            ),
        )

        receive_task = asyncio.create_task(websocket.receive_text())
        bus_task = asyncio.create_task(q.get())
        while True:
            done, _ = await asyncio.wait({receive_task, bus_task}, return_when=asyncio.FIRST_COMPLETED)

            if receive_task in done:
                try:
                    _ = receive_task.result()
                except WebSocketDisconnect:
                    break
                except Exception:
                    break
                receive_task = asyncio.create_task(websocket.receive_text())

            if bus_task in done:
                try:
                    event = bus_task.result()
                except Exception:
                    break

                if event.shell_id == shell_id:
                    if event.type == EventType.LOG_CHUNK:
                        stream = str(event.data.get("stream") or "stdout")
                        chunk = str(event.data.get("chunk") or "")
                        if stream in {"stdout", "stderr"} and chunk:
                            chunk_stream_name: LogStreamName = "stdout" if stream == "stdout" else "stderr"
                            await _send_ws_notification(
                                websocket,
                                build_logs_chunk_notification(shell_id, chunk_stream_name, chunk),
                            )
                    elif event.type == EventType.PTY_CHUNK:
                        chunk = str(event.data.get("chunk") or "")
                        if chunk:
                            await _send_ws_notification(websocket, build_logs_chunk_notification(shell_id, "stdout", chunk))
                    elif event.type == EventType.LOG_RESET:
                        stream = str(event.data.get("stream") or "stdout")
                        if stream in {"stdout", "stderr"}:
                            reset_stream_name: LogStreamName = "stdout" if stream == "stdout" else "stderr"
                            await _send_ws_notification(
                                websocket,
                                build_logs_reset_notification(shell_id, reset_stream_name),
                            )
                    elif event.type == EventType.SHELL_REMOVED:
                        await _send_ws_notification(
                            websocket,
                            build_error_notification("Shell removed", code="shell_removed", shell_id=shell_id),
                        )
                        break

                bus_task = asyncio.create_task(q.get())
    except Exception:
        pass
    finally:
        for task_name in ("receive_task", "bus_task"):
            task = locals().get(task_name)
            if isinstance(task, asyncio.Task):
                _ = task.cancel()
        try:
            bus.unsubscribe(q)
        except Exception:
            pass
        await safe_close()


# -----------------------------------------------------------------------------
# Compatibility routes (legacy TE2)


@router.get("/shell-logs/{shell_id}")
async def legacy_shell_logs(shell_id: str) -> RedirectResponse:
    return RedirectResponse(url=f"/fws/logs/{shell_id}", status_code=307)


@router.websocket("/ws/shell-logs/{shell_id}")
async def legacy_shell_logs_ws(websocket: WebSocket, shell_id: str):
    await fws_logs_ws(websocket, shell_id)
