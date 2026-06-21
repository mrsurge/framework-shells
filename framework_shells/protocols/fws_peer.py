from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, TypeAlias, TypeGuard, TypedDict, cast

from .fws_ui import (
    ERROR_METHOD,
    LOGS_CHUNK_METHOD,
    LOGS_IO_METADATA_METHOD,
    LOGS_RESET_METHOD,
    SHELL_CREATED_NOTIFICATION_METHOD,
    SHELL_EXITED_NOTIFICATION_METHOD,
    SHELL_INPUT_METHOD,
    SHELL_REMOVED_NOTIFICATION_METHOD,
    SHELL_SPAWNED_NOTIFICATION_METHOD,
    SHELL_UPDATED_NOTIFICATION_METHOD,
    FwsNotification,
)

FWS_SOCKETIO_NAMESPACE: Literal["/fws"] = "/fws"
FWS_SOCKETIO_SOCKET_PATH: Literal["/fws_ws/socket.io"] = "/fws_ws/socket.io"

FWS_BROWSER_ROLE: Literal["browser"] = "browser"
FWS_PEER_ROLE: Literal["peer"] = "peer"
FWS_DASHBOARD_ROOM: Literal["fws:dashboard"] = "fws:dashboard"
FWS_PEER_ROOM: Literal["fws:peers"] = "fws:peers"

FWS_REQUEST_EVENT: Literal["fws_request"] = "fws_request"
FWS_NOTIFICATION_EVENT: Literal["fws_notification"] = "fws_notification"
FWS_PEER_SUBSCRIPTIONS_EVENT: Literal["fws_peer_subscriptions"] = "fws_peer_subscriptions"
FWS_PEER_REQUEST_EVENT: Literal["fws_peer_request"] = "fws_peer_request"
FWS_PEER_NOTIFICATION_EVENT: Literal["fws_peer_notification"] = "fws_peer_notification"


class FwsPeerAuth(TypedDict):
    role: Literal["peer"]
    api_token: str
    runtime_id: str
    pid: str


class FwsPeerSubscriptionsPayload(TypedDict):
    shell_ids: list[str]


class _FwsPeerShellInputRequiredParams(TypedDict):
    shell_id: str


class FwsPeerShellInputParams(_FwsPeerShellInputRequiredParams, total=False):
    data: str
    append_newline: bool
    eof: bool
    source: str


class FwsPeerShellInputRequest(TypedDict):
    method: Literal["fws.shell.input"]
    params: FwsPeerShellInputParams


FwsPeerRequest: TypeAlias = FwsPeerShellInputRequest


class _FwsPeerSuccessResponseRequired(TypedDict):
    ok: Literal[True]


class FwsPeerSuccessResponse(_FwsPeerSuccessResponseRequired, total=False):
    data: dict[str, object]


class FwsPeerErrorResponse(TypedDict):
    ok: Literal[False]
    code: str
    error: str


FwsPeerResponse: TypeAlias = FwsPeerSuccessResponse | FwsPeerErrorResponse

FWS_PEER_NOTIFICATION_METHODS = frozenset(
    {
        SHELL_CREATED_NOTIFICATION_METHOD,
        SHELL_SPAWNED_NOTIFICATION_METHOD,
        SHELL_UPDATED_NOTIFICATION_METHOD,
        SHELL_EXITED_NOTIFICATION_METHOD,
        SHELL_REMOVED_NOTIFICATION_METHOD,
        LOGS_CHUNK_METHOD,
        LOGS_IO_METADATA_METHOD,
        LOGS_RESET_METHOD,
        ERROR_METHOD,
    }
)
FWS_PEER_SUBSCRIPTION_FILTERED_METHODS = frozenset({LOGS_CHUNK_METHOD, LOGS_RESET_METHOD, ERROR_METHOD})


def is_object_mapping(value: object) -> TypeGuard[Mapping[str, object]]:
    return isinstance(value, Mapping)


def shell_room(shell_id: str) -> str:
    return f"shell:{shell_id}"


def build_peer_auth(*, api_token: str, runtime_id: str, pid: str) -> FwsPeerAuth:
    return {
        "role": FWS_PEER_ROLE,
        "api_token": api_token,
        "runtime_id": runtime_id,
        "pid": pid,
    }


def build_peer_subscriptions(shell_ids: list[str]) -> FwsPeerSubscriptionsPayload:
    return {"shell_ids": list(shell_ids)}


def build_peer_shell_input_request(
    *,
    shell_id: str,
    data: str | None,
    append_newline: bool,
    eof: bool,
    source: str,
) -> FwsPeerShellInputRequest:
    params: FwsPeerShellInputParams = {
        "shell_id": shell_id,
        "data": data or "",
        "append_newline": append_newline,
        "eof": eof,
        "source": source,
    }
    return {"method": SHELL_INPUT_METHOD, "params": params}


def build_peer_success_response(data: Mapping[str, object] | None = None) -> FwsPeerSuccessResponse:
    if data is None:
        return {"ok": True}
    return {"ok": True, "data": dict(data)}


def build_peer_error_response(*, code: str, error: str) -> FwsPeerErrorResponse:
    return {"ok": False, "code": code, "error": error}


def parse_peer_subscriptions_payload(payload: object) -> FwsPeerSubscriptionsPayload | None:
    if not is_object_mapping(payload):
        return None
    shell_ids = payload.get("shell_ids")
    if not isinstance(shell_ids, list):
        return None
    return {
        "shell_ids": [
            shell_id
            for raw_shell_id in cast(list[object], shell_ids)
            if (shell_id := str(raw_shell_id).strip())
        ]
    }


def parse_peer_shell_input_request(payload: object) -> FwsPeerShellInputRequest | None:
    if not is_object_mapping(payload):
        return None
    if payload.get("method") != SHELL_INPUT_METHOD:
        return None
    raw_params = payload.get("params")
    if not is_object_mapping(raw_params):
        return None
    raw_shell_id = raw_params.get("shell_id")
    if not isinstance(raw_shell_id, str) or not raw_shell_id.strip():
        return None
    params: FwsPeerShellInputParams = {"shell_id": raw_shell_id.strip()}
    raw_data = raw_params.get("data")
    if isinstance(raw_data, str):
        params["data"] = raw_data
    raw_append_newline = raw_params.get("append_newline")
    if isinstance(raw_append_newline, bool):
        params["append_newline"] = raw_append_newline
    raw_eof = raw_params.get("eof")
    if isinstance(raw_eof, bool):
        params["eof"] = raw_eof
    raw_source = raw_params.get("source")
    if isinstance(raw_source, str) and raw_source.strip():
        params["source"] = raw_source
    return {"method": SHELL_INPUT_METHOD, "params": params}


def parse_peer_notification(payload: object) -> FwsNotification | None:
    if not is_object_mapping(payload):
        return None
    method = payload.get("method")
    params = payload.get("params")
    if payload.get("jsonrpc") != "2.0" or not isinstance(method, str) or not is_object_mapping(params):
        return None
    if method not in FWS_PEER_NOTIFICATION_METHODS:
        return None
    return cast(FwsNotification, cast(object, {"jsonrpc": "2.0", "method": method, "params": dict(params)}))


def notification_shell_id(notification: Mapping[str, object]) -> str | None:
    params = notification.get("params")
    if not is_object_mapping(params):
        return None
    shell_id = params.get("shell_id")
    return shell_id if isinstance(shell_id, str) else None


def peer_notification_requires_subscription(method: str) -> bool:
    return method in FWS_PEER_SUBSCRIPTION_FILTERED_METHODS
