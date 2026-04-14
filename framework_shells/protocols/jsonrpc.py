from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Final, Literal, TypeGuard, TypedDict, cast

JSONRPC_VERSION: Final[Literal["2.0"]] = "2.0"


class JsonRpcNotificationEnvelope(TypedDict):
    jsonrpc: Literal["2.0"]
    method: str
    params: dict[str, object]


@dataclass(frozen=True)
class ParsedJsonRpcNotification:
    method: str
    params: Mapping[str, object]


def is_object_mapping(value: object) -> TypeGuard[Mapping[str, object]]:
    return isinstance(value, Mapping)


def build_jsonrpc_notification(method: str, params: Mapping[str, object]) -> JsonRpcNotificationEnvelope:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": method,
        "params": dict(params),
    }


def dump_json_line(payload: Mapping[str, object]) -> str:
    return json.dumps(dict(payload), separators=(",", ":")) + "\n"


def parse_jsonrpc_notification(raw: str) -> ParsedJsonRpcNotification | None:
    try:
        payload_obj = cast(object, json.loads(raw))
    except json.JSONDecodeError:
        return None
    if not is_object_mapping(payload_obj):
        return None
    payload = payload_obj
    if payload.get("jsonrpc") != JSONRPC_VERSION:
        return None
    method = payload.get("method")
    params = payload.get("params")
    if not isinstance(method, str) or not is_object_mapping(params):
        return None
    return ParsedJsonRpcNotification(method=method, params=params)
