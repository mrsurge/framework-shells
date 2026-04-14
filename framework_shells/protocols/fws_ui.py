from __future__ import annotations

from typing import Literal, TypeAlias, TypedDict, cast

from .jsonrpc import (
    JSONRPC_VERSION,
    JsonRpcErrorResponseEnvelope,
    build_jsonrpc_error_response,
    build_jsonrpc_notification,
    parse_jsonrpc_request,
)

LogStreamName = Literal["stdout", "stderr"]
ShutdownScope = Literal["tree", "shells"]

DASHBOARD_OPEN_METHOD = "fws.dashboard.open"
LOGS_OPEN_METHOD = "fws.logs.open"
DASHBOARD_REFRESH_METHOD = "fws.dashboard.refresh"
LOGS_TRUNCATE_METHOD = "fws.logs.truncate"
EXITED_PURGE_METHOD = "fws.exited.purge"
SHELL_TERMINATE_METHOD = "fws.shell.terminate"
SHELL_PURGE_METHOD = "fws.shell.purge"
PID_TERMINATE_METHOD = "fws.pid.terminate"
APP_SHUTDOWN_METHOD = "fws.app.shutdown"
SHUTDOWN_METHOD = "fws.shutdown"

DASHBOARD_SNAPSHOT_METHOD = "fws.dashboard.snapshot"
LOGS_INITIAL_METHOD = "fws.logs.initial"
LOGS_CHUNK_METHOD = "fws.logs.chunk"
LOGS_RESET_METHOD = "fws.logs.reset"
ERROR_METHOD = "fws.error"


class DashboardOpenParams(TypedDict):
    view: Literal["html"]


class LogsOpenParams(TypedDict):
    shell_id: str


class EmptyParams(TypedDict):
    pass


class ShellActionParams(TypedDict):
    shell_id: str


class PidActionParams(TypedDict):
    pid: int


class AppActionParams(TypedDict):
    app_id: str


class ShutdownParams(TypedDict):
    scope: ShutdownScope


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


class OpenResult(TypedDict):
    accepted: Literal[True]


class LogsOpenResult(OpenResult):
    shell_id: str


class ActionResult(TypedDict):
    ok: Literal[True]


class DashboardOpenRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.dashboard.open"]
    id: str
    params: DashboardOpenParams


class LogsOpenRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.open"]
    id: str
    params: LogsOpenParams


class DashboardRefreshRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.dashboard.refresh"]
    id: str
    params: EmptyParams


class LogsTruncateRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.truncate"]
    id: str
    params: EmptyParams


class ExitedPurgeRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.exited.purge"]
    id: str
    params: EmptyParams


class ShellTerminateRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.terminate"]
    id: str
    params: ShellActionParams


class ShellPurgeRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.purge"]
    id: str
    params: ShellActionParams


class PidTerminateRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.pid.terminate"]
    id: str
    params: PidActionParams


class AppShutdownRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.app.shutdown"]
    id: str
    params: AppActionParams


class ShutdownRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shutdown"]
    id: str
    params: ShutdownParams


FwsRequest: TypeAlias = (
    DashboardOpenRequest
    | LogsOpenRequest
    | DashboardRefreshRequest
    | LogsTruncateRequest
    | ExitedPurgeRequest
    | ShellTerminateRequest
    | ShellPurgeRequest
    | PidTerminateRequest
    | AppShutdownRequest
    | ShutdownRequest
)


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


FwsNotification: TypeAlias = (
    DashboardSnapshotNotification
    | LogsInitialNotification
    | LogsChunkNotification
    | LogsResetNotification
    | ErrorNotification
)


class DashboardOpenResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: OpenResult


class LogsOpenResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: LogsOpenResult


class ActionResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: ActionResult


FwsSuccessResponse: TypeAlias = DashboardOpenResponse | LogsOpenResponse | ActionResponse
FwsErrorResponse: TypeAlias = JsonRpcErrorResponseEnvelope


def parse_fws_request(raw: str) -> FwsRequest | None:
    parsed = parse_jsonrpc_request(raw)
    if parsed is None:
        return None

    if parsed.method == DASHBOARD_OPEN_METHOD:
        if parsed.params.get("view") != "html":
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": DASHBOARD_OPEN_METHOD,
            "params": {"view": "html"},
        }

    if parsed.method == LOGS_OPEN_METHOD:
        shell_id = parsed.params.get("shell_id")
        if not isinstance(shell_id, str) or not shell_id.strip():
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": LOGS_OPEN_METHOD,
            "params": {"shell_id": shell_id},
        }

    if parsed.method == DASHBOARD_REFRESH_METHOD:
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": DASHBOARD_REFRESH_METHOD,
            "params": {},
        }

    if parsed.method == LOGS_TRUNCATE_METHOD:
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": LOGS_TRUNCATE_METHOD,
            "params": {},
        }

    if parsed.method == EXITED_PURGE_METHOD:
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": EXITED_PURGE_METHOD,
            "params": {},
        }

    if parsed.method == SHELL_TERMINATE_METHOD:
        shell_id = parsed.params.get("shell_id")
        if not isinstance(shell_id, str) or not shell_id.strip():
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": SHELL_TERMINATE_METHOD,
            "params": {"shell_id": shell_id},
        }

    if parsed.method == SHELL_PURGE_METHOD:
        shell_id = parsed.params.get("shell_id")
        if not isinstance(shell_id, str) or not shell_id.strip():
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": SHELL_PURGE_METHOD,
            "params": {"shell_id": shell_id},
        }

    if parsed.method == PID_TERMINATE_METHOD:
        pid = parsed.params.get("pid")
        if not isinstance(pid, int):
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": PID_TERMINATE_METHOD,
            "params": {"pid": pid},
        }

    if parsed.method == APP_SHUTDOWN_METHOD:
        app_id = parsed.params.get("app_id")
        if not isinstance(app_id, str) or not app_id.strip():
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": APP_SHUTDOWN_METHOD,
            "params": {"app_id": app_id},
        }

    if parsed.method == SHUTDOWN_METHOD:
        scope = parsed.params.get("scope")
        if scope not in {"tree", "shells"}:
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": SHUTDOWN_METHOD,
            "params": {"scope": cast(ShutdownScope, scope)},
        }

    return None


def build_dashboard_open_response(request_id: str) -> DashboardOpenResponse:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "result": {"accepted": True},
    }


def build_logs_open_response(request_id: str, shell_id: str) -> LogsOpenResponse:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "result": {"accepted": True, "shell_id": shell_id},
    }


def build_action_response(request_id: str) -> ActionResponse:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "result": {"ok": True},
    }


def build_request_error_response(
    request_id: str | None,
    *,
    code: int,
    message: str,
    error_code: str | None = None,
    shell_id: str | None = None,
) -> FwsErrorResponse:
    data: dict[str, object] = {}
    if error_code is not None:
        data["code"] = error_code
    if shell_id is not None:
        data["shell_id"] = shell_id
    return build_jsonrpc_error_response(
        request_id,
        code=code,
        message=message,
        data=data or None,
    )


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
        {"shell_id": shell_id, "stdout": stdout, "stderr": stderr},
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_INITIAL_METHOD,
        "params": {"shell_id": shell_id, "stdout": stdout, "stderr": stderr},
    }


def build_logs_chunk_notification(shell_id: str, stream: LogStreamName, chunk: str) -> LogsChunkNotification:
    notification = build_jsonrpc_notification(
        LOGS_CHUNK_METHOD,
        {"shell_id": shell_id, "stream": stream, "chunk": chunk},
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_CHUNK_METHOD,
        "params": {"shell_id": shell_id, "stream": stream, "chunk": chunk},
    }


def build_logs_reset_notification(shell_id: str, stream: LogStreamName) -> LogsResetNotification:
    notification = build_jsonrpc_notification(
        LOGS_RESET_METHOD,
        {"shell_id": shell_id, "stream": stream},
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_RESET_METHOD,
        "params": {"shell_id": shell_id, "stream": stream},
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
