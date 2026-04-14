from __future__ import annotations

from typing import Literal, TypedDict

from .jsonrpc import JSONRPC_VERSION, build_jsonrpc_notification, parse_jsonrpc_notification

LogStreamName = Literal["stdout", "stderr"]

DASHBOARD_CONNECT_METHOD = "fws.dashboard.connect"
LOGS_CONNECT_METHOD = "fws.logs.connect"
DASHBOARD_SNAPSHOT_METHOD = "fws.dashboard.snapshot"
LOGS_INITIAL_METHOD = "fws.logs.initial"
LOGS_CHUNK_METHOD = "fws.logs.chunk"
LOGS_RESET_METHOD = "fws.logs.reset"
ERROR_METHOD = "fws.error"


class DashboardConnectParams(TypedDict):
    view: Literal["html"]


class LogsConnectParams(TypedDict):
    shell_id: str


class DashboardSnapshotParams(TypedDict):
    html: str


class LogsInitialParams(TypedDict):
    shell_id: str
    stdout: str
    stderr: str


class LogsChunkParams(TypedDict):
    shell_id: str
    stream: LogStreamName
    chunk: str


class LogsResetParams(TypedDict):
    shell_id: str
    stream: LogStreamName


class ErrorNotificationParams(TypedDict, total=False):
    message: str
    code: str
    shell_id: str


class DashboardConnectNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.dashboard.connect"]
    params: DashboardConnectParams


class LogsConnectNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.connect"]
    params: LogsConnectParams


class DashboardSnapshotNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.dashboard.snapshot"]
    params: DashboardSnapshotParams


class LogsInitialNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.initial"]
    params: LogsInitialParams


class LogsChunkNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.chunk"]
    params: LogsChunkParams


class LogsResetNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.reset"]
    params: LogsResetParams


class ErrorNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.error"]
    params: ErrorNotificationParams


FwsClientNotification = DashboardConnectNotification | LogsConnectNotification
FwsServerNotification = (
    DashboardSnapshotNotification
    | LogsInitialNotification
    | LogsChunkNotification
    | LogsResetNotification
    | ErrorNotification
)


def parse_dashboard_connect_notification(raw: str) -> DashboardConnectNotification | None:
    parsed = parse_jsonrpc_notification(raw)
    if parsed is None or parsed.method != DASHBOARD_CONNECT_METHOD:
        return None
    if parsed.params.get("view") != "html":
        return None
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": DASHBOARD_CONNECT_METHOD,
        "params": {"view": "html"},
    }


def parse_logs_connect_notification(raw: str) -> LogsConnectNotification | None:
    parsed = parse_jsonrpc_notification(raw)
    if parsed is None or parsed.method != LOGS_CONNECT_METHOD:
        return None
    shell_id = parsed.params.get("shell_id")
    if not isinstance(shell_id, str) or not shell_id.strip():
        return None
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": LOGS_CONNECT_METHOD,
        "params": {"shell_id": shell_id},
    }


def build_dashboard_snapshot_notification(html: str) -> DashboardSnapshotNotification:
    notification = build_jsonrpc_notification(DASHBOARD_SNAPSHOT_METHOD, {"html": html})
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": DASHBOARD_SNAPSHOT_METHOD,
        "params": {"html": html},
    }


def build_logs_initial_notification(shell_id: str, stdout: str, stderr: str) -> LogsInitialNotification:
    notification = build_jsonrpc_notification(
        LOGS_INITIAL_METHOD,
        {
            "shell_id": shell_id,
            "stdout": stdout,
            "stderr": stderr,
        },
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_INITIAL_METHOD,
        "params": {
            "shell_id": shell_id,
            "stdout": stdout,
            "stderr": stderr,
        },
    }


def build_logs_chunk_notification(shell_id: str, stream: LogStreamName, chunk: str) -> LogsChunkNotification:
    notification = build_jsonrpc_notification(
        LOGS_CHUNK_METHOD,
        {
            "shell_id": shell_id,
            "stream": stream,
            "chunk": chunk,
        },
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_CHUNK_METHOD,
        "params": {
            "shell_id": shell_id,
            "stream": stream,
            "chunk": chunk,
        },
    }


def build_logs_reset_notification(shell_id: str, stream: LogStreamName) -> LogsResetNotification:
    notification = build_jsonrpc_notification(
        LOGS_RESET_METHOD,
        {
            "shell_id": shell_id,
            "stream": stream,
        },
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_RESET_METHOD,
        "params": {
            "shell_id": shell_id,
            "stream": stream,
        },
    }


def build_error_notification(
    message: str,
    *,
    code: str | None = None,
    shell_id: str | None = None,
) -> ErrorNotification:
    params: ErrorNotificationParams = {"message": message}
    if code is not None:
        params["code"] = code
    if shell_id is not None:
        params["shell_id"] = shell_id
    notification = build_jsonrpc_notification(ERROR_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": ERROR_METHOD,
        "params": params,
    }
