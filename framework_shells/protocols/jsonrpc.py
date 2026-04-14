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


class JsonRpcRequestEnvelope(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    method: str
    params: dict[str, object]


class JsonRpcSuccessResponseEnvelope(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str
    result: dict[str, object]


class JsonRpcErrorObject(TypedDict, total=False):
    code: int
    message: str
    data: dict[str, object]


class JsonRpcErrorResponseEnvelope(TypedDict):
    jsonrpc: Literal["2.0"]
    id: str | None
    error: JsonRpcErrorObject


@dataclass(frozen=True)
class ParsedJsonRpcNotification:
    method: str
    params: Mapping[str, object]


@dataclass(frozen=True)
class ParsedJsonRpcRequest:
    id: str
    method: str
    params: Mapping[str, object]


@dataclass(frozen=True)
class ParsedJsonRpcError:
    code: int
    message: str
    data: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ParsedJsonRpcSuccessResponse:
    id: str
    result: Mapping[str, object]


@dataclass(frozen=True)
class ParsedJsonRpcErrorResponse:
    id: str | None
    error: ParsedJsonRpcError


def is_object_mapping(value: object) -> TypeGuard[Mapping[str, object]]:
    return isinstance(value, Mapping)


def build_jsonrpc_notification(method: str, params: Mapping[str, object]) -> JsonRpcNotificationEnvelope:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": method,
        "params": dict(params),
    }


def build_jsonrpc_request(id: str, method: str, params: Mapping[str, object]) -> JsonRpcRequestEnvelope:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "method": method,
        "params": dict(params),
    }


def build_jsonrpc_success_response(id: str, result: Mapping[str, object]) -> JsonRpcSuccessResponseEnvelope:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "result": dict(result),
    }


def build_jsonrpc_error_response(
    id: str | None,
    *,
    code: int,
    message: str,
    data: Mapping[str, object] | None = None,
) -> JsonRpcErrorResponseEnvelope:
    error: JsonRpcErrorObject = {
        "code": code,
        "message": message,
    }
    if data is not None:
        error["data"] = dict(data)
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": error,
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


def parse_jsonrpc_request(raw: str) -> ParsedJsonRpcRequest | None:
    try:
        payload_obj = cast(object, json.loads(raw))
    except json.JSONDecodeError:
        return None
    if not is_object_mapping(payload_obj):
        return None
    payload = payload_obj
    if payload.get("jsonrpc") != JSONRPC_VERSION:
        return None
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params")
    if not isinstance(request_id, str) or not isinstance(method, str) or not is_object_mapping(params):
        return None
    return ParsedJsonRpcRequest(id=request_id, method=method, params=params)


def parse_jsonrpc_success_response(raw: str) -> ParsedJsonRpcSuccessResponse | None:
    try:
        payload_obj = cast(object, json.loads(raw))
    except json.JSONDecodeError:
        return None
    if not is_object_mapping(payload_obj):
        return None
    payload = payload_obj
    if payload.get("jsonrpc") != JSONRPC_VERSION:
        return None
    response_id = payload.get("id")
    result = payload.get("result")
    if not isinstance(response_id, str) or not is_object_mapping(result):
        return None
    return ParsedJsonRpcSuccessResponse(id=response_id, result=result)


def parse_jsonrpc_error_response(raw: str) -> ParsedJsonRpcErrorResponse | None:
    try:
        payload_obj = cast(object, json.loads(raw))
    except json.JSONDecodeError:
        return None
    if not is_object_mapping(payload_obj):
        return None
    payload = payload_obj
    if payload.get("jsonrpc") != JSONRPC_VERSION:
        return None
    response_id_obj = payload.get("id")
    error_obj = payload.get("error")
    if response_id_obj is not None and not isinstance(response_id_obj, str):
        return None
    if not is_object_mapping(error_obj):
        return None
    code = error_obj.get("code")
    message = error_obj.get("message")
    data = error_obj.get("data")
    if not isinstance(code, int) or not isinstance(message, str):
        return None
    error_data = data if is_object_mapping(data) else None
    return ParsedJsonRpcErrorResponse(
        id=response_id_obj,
        error=ParsedJsonRpcError(code=code, message=message, data=error_data),
    )
