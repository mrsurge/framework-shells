from __future__ import annotations

from typing import Literal, TypeAlias, TypedDict, cast

from .jsonrpc import (
    JSONRPC_VERSION,
    JsonRpcErrorResponseEnvelope,
    build_jsonrpc_error_response,
    build_jsonrpc_notification,
    build_jsonrpc_success_response,
    parse_jsonrpc_request,
)

LogStreamName = Literal["stdout", "stderr"]
ShutdownScope = Literal["tree", "shells"]
FwsShellEventMethod = Literal[
    "fws.shell.created",
    "fws.shell.spawned",
    "fws.shell.updated",
    "fws.shell.exited",
]

DASHBOARD_OPEN_METHOD = "fws.dashboard.open"
LOGS_OPEN_METHOD = "fws.logs.open"
LOGS_CLOSE_METHOD = "fws.logs.close"
DASHBOARD_REFRESH_METHOD = "fws.dashboard.refresh"
LOGS_TRUNCATE_METHOD = "fws.logs.truncate"
EXITED_PURGE_METHOD = "fws.exited.purge"
SHELL_TERMINATE_METHOD = "fws.shell.terminate"
SHELL_PURGE_METHOD = "fws.shell.purge"
SHELL_INPUT_METHOD = "fws.shell.input"
PID_TERMINATE_METHOD = "fws.pid.terminate"
APP_SHUTDOWN_METHOD = "fws.app.shutdown"
SHUTDOWN_METHOD = "fws.shutdown"

SHELL_CREATED_NOTIFICATION_METHOD: Literal["fws.shell.created"] = "fws.shell.created"
SHELL_SPAWNED_NOTIFICATION_METHOD: Literal["fws.shell.spawned"] = "fws.shell.spawned"
SHELL_UPDATED_NOTIFICATION_METHOD: Literal["fws.shell.updated"] = "fws.shell.updated"
SHELL_EXITED_NOTIFICATION_METHOD: Literal["fws.shell.exited"] = "fws.shell.exited"
SHELL_REMOVED_NOTIFICATION_METHOD: Literal["fws.shell.removed"] = "fws.shell.removed"
LOGS_INITIAL_METHOD = "fws.logs.initial"
LOGS_CHUNK_METHOD = "fws.logs.chunk"
LOGS_IO_METADATA_METHOD = "fws.logs.io_metadata"
LOGS_RESET_METHOD = "fws.logs.reset"
ERROR_METHOD = "fws.error"


class DashboardOpenParams(TypedDict):
    view: Literal["html"]


class LogsOpenParams(TypedDict):
    shell_id: str


class LogsCloseParams(TypedDict):
    shell_id: str


class EmptyParams(TypedDict):
    pass


class ShellActionParams(TypedDict):
    shell_id: str


class ShellInputParams(TypedDict, total=False):
    shell_id: str
    data: str
    append_newline: bool
    eof: bool


class PidActionParams(TypedDict):
    pid: int


class AppActionParams(TypedDict):
    app_id: str


class ShutdownParams(TypedDict):
    scope: ShutdownScope


class DashboardShellStats(TypedDict, total=False):
    alive: bool
    uptime: float | None
    cpu_percent: float
    memory_rss: int


class DashboardShellCapabilities(TypedDict, total=False):
    backend: str
    stdin_write: bool
    stdin_eof: bool
    stdout_subscribe: bool
    stdout_subscribe_bytes: bool
    stderr_subscribe: bool
    resize: bool
    reattach: bool


class DashboardPipeRuntime(TypedDict, total=False):
    engine: str
    active: bool
    phase: str


class DashboardShellPayload(TypedDict, total=False):
    id: str
    spec_id: str | None
    command: list[str]
    label: str | None
    subgroups: list[str]
    ui: dict[str, object]
    cwd: str
    pid: int | None
    status: str
    created_at: float
    updated_at: float
    autostart: bool
    stdout_log: str
    stderr_log: str
    exit_code: int | None
    env_keys: list[str]
    run_id: str | None
    launcher_pid: int | None
    adopted: bool
    backend: str
    uses_pty: bool
    uses_pipes: bool
    uses_dtach: bool
    pty_mode: str
    runtime_id: str | None
    app_id: str | None
    parent_shell_id: str | None
    is_app_worker: bool
    stats: DashboardShellStats
    capabilities: DashboardShellCapabilities
    pipe_runtime: DashboardPipeRuntime


class DashboardProcessPayload(TypedDict, total=False):
    pid: int
    parent_pid: int | None
    type: str
    label: str | None
    shell_id: str | None
    metadata: dict[str, object]


class DashboardStatePayload(TypedDict):
    shells: list[DashboardShellPayload]
    processes: list[DashboardProcessPayload]


class LogsInitialParams(TypedDict):
    shell_id: str
    stdout: str
    stderr: str
    io_metadata: list["IoMetadataPayload"]


class LogsChunkParams(TypedDict):
    shell_id: str
    stream: LogStreamName
    chunk: str


class IoMetadataPayload(TypedDict, total=False):
    schema: str
    shell_id: str
    kind: str
    stream: str
    ts: float
    source: str
    backend: str
    byte_start: int
    byte_end: int
    byte_count: int
    append_newline: bool
    newline_appended: bool
    preview: str
    text: str
    preview_truncated: bool
    sha256: str


class LogsIoMetadataParams(TypedDict):
    shell_id: str
    record: IoMetadataPayload


class LogsResetParams(TypedDict):
    shell_id: str
    stream: LogStreamName


class ShellEventParams(TypedDict):
    shell: DashboardShellPayload


class ShellRemovedParams(TypedDict):
    shell_id: str


class ErrorNotificationParams(TypedDict, total=False):
    message: str
    code: str
    shell_id: str


class DashboardOpenResult(TypedDict):
    accepted: Literal[True]
    state: DashboardStatePayload


class DashboardRefreshResult(TypedDict):
    ok: Literal[True]
    state: DashboardStatePayload


class LogsOpenResult(TypedDict):
    accepted: Literal[True]
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


class LogsCloseRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.close"]
    id: str
    params: LogsCloseParams


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


class ShellInputRequest(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.input"]
    id: str
    params: ShellInputParams


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
    | LogsCloseRequest
    | DashboardRefreshRequest
    | LogsTruncateRequest
    | ExitedPurgeRequest
    | ShellTerminateRequest
    | ShellPurgeRequest
    | ShellInputRequest
    | PidTerminateRequest
    | AppShutdownRequest
    | ShutdownRequest
)


class ShellCreatedNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.created"]
    params: ShellEventParams


class ShellSpawnedNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.spawned"]
    params: ShellEventParams


class ShellUpdatedNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.updated"]
    params: ShellEventParams


class ShellExitedNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.exited"]
    params: ShellEventParams


class ShellRemovedNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.shell.removed"]
    params: ShellRemovedParams


class LogsInitialNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.initial"]
    params: LogsInitialParams


class LogsChunkNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.chunk"]
    params: LogsChunkParams


class LogsIoMetadataNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.io_metadata"]
    params: LogsIoMetadataParams


class LogsResetNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.logs.reset"]
    params: LogsResetParams


class ErrorNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["fws.error"]
    params: ErrorNotificationParams


FwsNotification: TypeAlias = (
    ShellCreatedNotification
    | ShellSpawnedNotification
    | ShellUpdatedNotification
    | ShellExitedNotification
    | ShellRemovedNotification
    | LogsInitialNotification
    | LogsChunkNotification
    | LogsIoMetadataNotification
    | LogsResetNotification
    | ErrorNotification
)


class DashboardOpenResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: DashboardOpenResult


class DashboardRefreshResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: DashboardRefreshResult


class LogsOpenResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: LogsOpenResult


class ActionResponse(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: ActionResult


FwsSuccessResponse: TypeAlias = DashboardOpenResponse | DashboardRefreshResponse | LogsOpenResponse | ActionResponse
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

    if parsed.method == LOGS_CLOSE_METHOD:
        shell_id = parsed.params.get("shell_id")
        if not isinstance(shell_id, str) or not shell_id.strip():
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": LOGS_CLOSE_METHOD,
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

    if parsed.method == SHELL_INPUT_METHOD:
        shell_id = parsed.params.get("shell_id")
        eof = parsed.params.get("eof")
        data = parsed.params.get("data")
        append_newline = parsed.params.get("append_newline")
        if not isinstance(shell_id, str) or not shell_id.strip():
            return None
        if eof is not None and not isinstance(eof, bool):
            return None
        if append_newline is not None and not isinstance(append_newline, bool):
            return None
        if data is not None and not isinstance(data, str):
            return None
        if eof is True and data not in (None, ""):
            return None
        if eof is not True and data is None:
            return None
        params: ShellInputParams = {"shell_id": shell_id}
        if isinstance(data, str):
            params["data"] = data
        if isinstance(append_newline, bool):
            params["append_newline"] = append_newline
        if isinstance(eof, bool):
            params["eof"] = eof
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": parsed.id,
            "method": SHELL_INPUT_METHOD,
            "params": params,
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


def build_dashboard_state_payload(
    shells: list[DashboardShellPayload],
    processes: list[DashboardProcessPayload],
) -> DashboardStatePayload:
    return {
        "shells": list(shells),
        "processes": list(processes),
    }


def build_dashboard_open_response(
    request_id: str,
    shells: list[DashboardShellPayload],
    processes: list[DashboardProcessPayload],
) -> DashboardOpenResponse:
    result: DashboardOpenResult = {
        "accepted": True,
        "state": build_dashboard_state_payload(shells, processes),
    }
    response = build_jsonrpc_success_response(request_id, result)
    return {
        "jsonrpc": response["jsonrpc"],
        "id": request_id,
        "result": result,
    }


def build_dashboard_refresh_response(
    request_id: str,
    shells: list[DashboardShellPayload],
    processes: list[DashboardProcessPayload],
) -> DashboardRefreshResponse:
    result: DashboardRefreshResult = {
        "ok": True,
        "state": build_dashboard_state_payload(shells, processes),
    }
    response = build_jsonrpc_success_response(request_id, result)
    return {
        "jsonrpc": response["jsonrpc"],
        "id": request_id,
        "result": result,
    }


def build_logs_open_response(request_id: str, shell_id: str) -> LogsOpenResponse:
    result: LogsOpenResult = {"accepted": True, "shell_id": shell_id}
    response = build_jsonrpc_success_response(request_id, result)
    return {
        "jsonrpc": response["jsonrpc"],
        "id": request_id,
        "result": result,
    }


def build_action_response(request_id: str) -> ActionResponse:
    result: ActionResult = {"ok": True}
    response = build_jsonrpc_success_response(request_id, result)
    return {
        "jsonrpc": response["jsonrpc"],
        "id": request_id,
        "result": result,
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


def build_shell_event_notification(method: FwsShellEventMethod, shell: DashboardShellPayload) -> FwsNotification:
    params: ShellEventParams = {"shell": shell}
    notification = build_jsonrpc_notification(method, params)
    return cast(
        FwsNotification,
        cast(
            object,
            {
                "jsonrpc": notification["jsonrpc"],
                "method": method,
                "params": params,
            },
        ),
    )


def build_shell_removed_notification(shell_id: str) -> ShellRemovedNotification:
    notification = build_jsonrpc_notification(SHELL_REMOVED_NOTIFICATION_METHOD, {"shell_id": shell_id})
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": SHELL_REMOVED_NOTIFICATION_METHOD,
        "params": {"shell_id": shell_id},
    }


def build_logs_initial_notification(
    shell_id: str,
    stdout: str,
    stderr: str,
    *,
    io_metadata: list[IoMetadataPayload] | None = None,
) -> LogsInitialNotification:
    params: LogsInitialParams = {
        "shell_id": shell_id,
        "stdout": stdout,
        "stderr": stderr,
        "io_metadata": list(io_metadata or []),
    }
    notification = build_jsonrpc_notification(
        LOGS_INITIAL_METHOD,
        params,
    )
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_INITIAL_METHOD,
        "params": params,
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


def build_logs_io_metadata_notification(shell_id: str, record: IoMetadataPayload) -> LogsIoMetadataNotification:
    params: LogsIoMetadataParams = {"shell_id": shell_id, "record": record}
    notification = build_jsonrpc_notification(LOGS_IO_METADATA_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": LOGS_IO_METADATA_METHOD,
        "params": params,
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
