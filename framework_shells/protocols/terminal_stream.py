from __future__ import annotations

import base64
import json
from typing import Literal, TypeAlias, TypedDict, cast

from .jsonrpc import JSONRPC_VERSION, build_jsonrpc_notification, is_object_mapping, parse_jsonrpc_notification

TERMINAL_CONNECT_METHOD = "terminal.connect"
TERMINAL_INPUT_METHOD = "terminal.input"
TERMINAL_RESIZE_METHOD = "terminal.resize"
TERMINAL_DESTROY_METHOD = "terminal.destroy"
TERMINAL_PING_METHOD = "terminal.ping"

TERMINAL_READY_EVENT = "ready"
TERMINAL_DATA_EVENT = "data"
TERMINAL_PONG_EVENT = "pong"
TERMINAL_CLOSED_EVENT = "closed"

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class TerminalConnectParams(TypedDict, total=False):
    cols: int
    rows: int


class TerminalInputParams(TypedDict):
    data_b64: str


class TerminalResizeParams(TypedDict):
    cols: int
    rows: int


class TerminalDestroyParams(TypedDict):
    pass


class TerminalPingParams(TypedDict, total=False):
    nonce: JsonValue


class TerminalConnectNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["terminal.connect"]
    params: TerminalConnectParams


class TerminalInputNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["terminal.input"]
    params: TerminalInputParams


class TerminalResizeNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["terminal.resize"]
    params: TerminalResizeParams


class TerminalDestroyNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["terminal.destroy"]
    params: TerminalDestroyParams


class TerminalPingNotification(TypedDict):
    jsonrpc: Literal["2.0"]
    method: Literal["terminal.ping"]
    params: TerminalPingParams


TerminalClientNotification: TypeAlias = (
    TerminalConnectNotification
    | TerminalInputNotification
    | TerminalResizeNotification
    | TerminalDestroyNotification
    | TerminalPingNotification
)


class TerminalReadyEventFrame(TypedDict):
    type: Literal["ready"]
    ts: int
    pid: int
    shell: list[str]
    cwd: str


class TerminalDataEventFrame(TypedDict):
    type: Literal["data"]
    seq: int
    ts: int
    data_b64: str


class TerminalPongEventFrame(TypedDict):
    type: Literal["pong"]
    nonce: JsonValue | None


class TerminalClosedEventFrame(TypedDict):
    type: Literal["closed"]
    seq: int
    ts: int
    exit_code: int | None
    reason: str


TerminalServerEventFrame: TypeAlias = (
    TerminalReadyEventFrame
    | TerminalDataEventFrame
    | TerminalPongEventFrame
    | TerminalClosedEventFrame
)


def encode_terminal_input_bytes(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def decode_terminal_input_bytes(data_b64: str) -> bytes:
    return base64.b64decode(data_b64)


def build_terminal_connect_notification(*, cols: int | None = None, rows: int | None = None) -> TerminalConnectNotification:
    params: TerminalConnectParams = {}
    if cols is not None:
        params["cols"] = cols
    if rows is not None:
        params["rows"] = rows
    notification = build_jsonrpc_notification(TERMINAL_CONNECT_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": TERMINAL_CONNECT_METHOD,
        "params": params,
    }


def build_terminal_input_notification(payload: bytes) -> TerminalInputNotification:
    params: TerminalInputParams = {"data_b64": encode_terminal_input_bytes(payload)}
    notification = build_jsonrpc_notification(TERMINAL_INPUT_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": TERMINAL_INPUT_METHOD,
        "params": params,
    }


def build_terminal_resize_notification(cols: int, rows: int) -> TerminalResizeNotification:
    params: TerminalResizeParams = {
        "cols": cols,
        "rows": rows,
    }
    notification = build_jsonrpc_notification(TERMINAL_RESIZE_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": TERMINAL_RESIZE_METHOD,
        "params": params,
    }


def build_terminal_destroy_notification() -> TerminalDestroyNotification:
    notification = build_jsonrpc_notification(TERMINAL_DESTROY_METHOD, {})
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": TERMINAL_DESTROY_METHOD,
        "params": {},
    }


def build_terminal_ping_notification(nonce: JsonValue | None = None) -> TerminalPingNotification:
    params: TerminalPingParams = {}
    if nonce is not None:
        params["nonce"] = nonce
    notification = build_jsonrpc_notification(TERMINAL_PING_METHOD, params)
    return {
        "jsonrpc": notification["jsonrpc"],
        "method": TERMINAL_PING_METHOD,
        "params": params,
    }


def parse_terminal_client_notification(raw: str) -> TerminalClientNotification | None:
    parsed = parse_jsonrpc_notification(raw)
    if parsed is None:
        return None

    if parsed.method == TERMINAL_CONNECT_METHOD:
        connect_params: TerminalConnectParams = {}
        cols = parsed.params.get("cols")
        rows = parsed.params.get("rows")
        if isinstance(cols, int):
            connect_params["cols"] = cols
        if isinstance(rows, int):
            connect_params["rows"] = rows
        return {
            "jsonrpc": JSONRPC_VERSION,
            "method": TERMINAL_CONNECT_METHOD,
            "params": connect_params,
        }

    if parsed.method == TERMINAL_INPUT_METHOD:
        data_b64 = parsed.params.get("data_b64")
        if not isinstance(data_b64, str) or not data_b64:
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "method": TERMINAL_INPUT_METHOD,
            "params": {"data_b64": data_b64},
        }

    if parsed.method == TERMINAL_RESIZE_METHOD:
        cols = parsed.params.get("cols")
        rows = parsed.params.get("rows")
        if not isinstance(cols, int) or not isinstance(rows, int):
            return None
        return {
            "jsonrpc": JSONRPC_VERSION,
            "method": TERMINAL_RESIZE_METHOD,
            "params": {
                "cols": cols,
                "rows": rows,
            },
        }

    if parsed.method == TERMINAL_DESTROY_METHOD:
        return {
            "jsonrpc": JSONRPC_VERSION,
            "method": TERMINAL_DESTROY_METHOD,
            "params": {},
        }

    if parsed.method == TERMINAL_PING_METHOD:
        ping_params: TerminalPingParams = {}
        nonce = parsed.params.get("nonce")
        if nonce is not None:
            ping_params["nonce"] = cast(JsonValue, nonce)
        return {
            "jsonrpc": JSONRPC_VERSION,
            "method": TERMINAL_PING_METHOD,
            "params": ping_params,
        }

    return None


def build_terminal_ready_event(*, ts: int, pid: int, shell: list[str], cwd: str) -> TerminalReadyEventFrame:
    return {
        "type": TERMINAL_READY_EVENT,
        "ts": ts,
        "pid": pid,
        "shell": shell,
        "cwd": cwd,
    }


def build_terminal_data_event(*, seq: int, ts: int, payload: bytes) -> TerminalDataEventFrame:
    return {
        "type": TERMINAL_DATA_EVENT,
        "seq": seq,
        "ts": ts,
        "data_b64": encode_terminal_input_bytes(payload),
    }


def build_terminal_pong_event(nonce: JsonValue | None) -> TerminalPongEventFrame:
    return {
        "type": TERMINAL_PONG_EVENT,
        "nonce": nonce,
    }


def build_terminal_closed_event(*, seq: int, ts: int, exit_code: int | None, reason: str) -> TerminalClosedEventFrame:
    return {
        "type": TERMINAL_CLOSED_EVENT,
        "seq": seq,
        "ts": ts,
        "exit_code": exit_code,
        "reason": reason,
    }


def parse_terminal_server_event(raw: str) -> TerminalServerEventFrame | None:
    try:
        payload_obj = cast(object, json.loads(raw))
    except json.JSONDecodeError:
        return None
    if not is_object_mapping(payload_obj):
        return None
    payload = payload_obj
    event_type = payload.get("type")

    if event_type == TERMINAL_READY_EVENT:
        ts = payload.get("ts")
        pid = payload.get("pid")
        shell = payload.get("shell")
        cwd = payload.get("cwd")
        if (
            isinstance(ts, int)
            and isinstance(pid, int)
            and isinstance(shell, list)
            and all(isinstance(part, str) for part in cast(list[object], shell))
            and isinstance(cwd, str)
        ):
            shell_parts = cast(list[str], shell)
            return {
                "type": TERMINAL_READY_EVENT,
                "ts": ts,
                "pid": pid,
                "shell": shell_parts,
                "cwd": cwd,
            }
        return None

    if event_type == TERMINAL_DATA_EVENT:
        seq = payload.get("seq")
        ts = payload.get("ts")
        data_b64 = payload.get("data_b64")
        if isinstance(seq, int) and isinstance(ts, int) and isinstance(data_b64, str):
            return {
                "type": TERMINAL_DATA_EVENT,
                "seq": seq,
                "ts": ts,
                "data_b64": data_b64,
            }
        return None

    if event_type == TERMINAL_PONG_EVENT:
        nonce = payload.get("nonce")
        return {
            "type": TERMINAL_PONG_EVENT,
            "nonce": cast(JsonValue | None, nonce),
        }

    if event_type == TERMINAL_CLOSED_EVENT:
        seq = payload.get("seq")
        ts = payload.get("ts")
        exit_code = payload.get("exit_code")
        reason = payload.get("reason")
        if (
            isinstance(seq, int)
            and isinstance(ts, int)
            and (exit_code is None or isinstance(exit_code, int))
            and isinstance(reason, str)
        ):
            return {
                "type": TERMINAL_CLOSED_EVENT,
                "seq": seq,
                "ts": ts,
                "exit_code": exit_code,
                "reason": reason,
            }
        return None

    return None
