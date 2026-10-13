"""Typed ingress and response helpers; routes and runtime behavior stay app-owned."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
import re
from typing import cast

import msgspec
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

JsonDict = dict[str, object]
HttpHandler = Callable[[Request], Awaitable[object]]
MAX_BODY_BYTES = 8 * 1024 * 1024


class RequestError(Exception):
    def __init__(self, errors: list[JsonDict]) -> None:
        super().__init__("Invalid request")
        self.errors = errors


def validation_error(kind: str, location: list[str], message: str, value: object,
                     context: JsonDict | None = None) -> JsonDict:
    result: JsonDict = {"type": kind, "loc": location, "msg": message, "input": value}
    if context is not None:
        result["ctx"] = context
    return result


async def read_body(request: Request, *, limit: int = MAX_BODY_BYTES) -> bytes:
    length = request.headers.get("content-length")
    if length is not None:
        try:
            size = int(length)
        except ValueError as exc:
            raise HTTPException(400, "Invalid Content-Length") from exc
        if size < 0:
            raise HTTPException(400, "Invalid Content-Length")
        if size > limit:
            raise HTTPException(413, "Request body too large")
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > limit:
            raise HTTPException(413, "Request body too large")
        data.extend(chunk)
    return bytes(data)


async def read_json_object(request: Request) -> JsonDict:
    data = await read_body(request)
    if not data:
        raise RequestError([validation_error("missing", ["body"], "Field required", None)])
    content_type = request.headers.get("content-type", "application/json").split(";", 1)[0].strip().lower()
    if content_type != "application/json" and not content_type.endswith("+json"):
        raise HTTPException(422, "Request body must be JSON")
    try:
        value: object = msgspec.json.decode(data)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise RequestError([validation_error("json_invalid", ["body"], "JSON decode error", None,
                                           {"error": str(exc)})]) from exc
    if value is None:
        raise RequestError([validation_error("missing", ["body"], "Field required", None)])
    if not isinstance(value, dict):
        raise RequestError([validation_error("dict_type", ["body"],
                                           "Input should be a valid dictionary", value)])
    return cast(JsonDict, value)


class Query:
    def __init__(self, request: Request) -> None:
        self.values: Mapping[str, str] = request.query_params
        self.errors: list[JsonDict] = []

    def text(self, name: str, default: str | None = None) -> str | None:
        return self.values.get(name, default)

    def required(self, name: str, *, minimum_length: int = 0) -> str:
        value = self.values.get(name)
        if value is None:
            self.errors.append(validation_error("missing", ["query", name], "Field required", None))
            return ""
        if len(value) < minimum_length:
            self.errors.append(validation_error("string_too_short", ["query", name],
                f"String should have at least {minimum_length} character", value,
                {"min_length": minimum_length}))
        return value

    def boolean(self, name: str, default: bool = False) -> bool:
        value = self.values.get(name)
        if value is None:
            return default
        normalized = value.lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "off"}:
            return False
        self.errors.append(validation_error("bool_parsing", ["query", name],
            "Input should be a valid boolean, unable to interpret input", value))
        return default

    def integer(self, name: str, default: int | None = None, *, minimum: int | None = None,
                maximum: int | None = None) -> int:
        value = self.values.get(name)
        if value is None:
            if default is not None:
                return default
            self.errors.append(validation_error("missing", ["query", name], "Field required", None))
            return 0
        try:
            if not re.fullmatch(r"[+-]?\d+(?:\.0+)?", value.strip()):
                raise ValueError()
            parsed = int(value.split(".", 1)[0])
        except ValueError:
            self.errors.append(validation_error("int_parsing", ["query", name],
                "Input should be a valid integer, unable to parse string as an integer", value))
            return 0
        for bound, kind, symbol, message in (
            (minimum, "greater_than_equal", "ge", "greater than or equal to"),
            (maximum, "less_than_equal", "le", "less than or equal to"),
        ):
            if bound is not None and ((symbol == "ge" and parsed < bound) or (symbol == "le" and parsed > bound)):
                self.errors.append(validation_error(kind, ["query", name],
                    f"Input should be {message} {bound}", value, {symbol: bound}))
        return parsed

    def literal(self, name: str, choices: Sequence[str], default: str) -> str:
        value = self.values.get(name, default)
        if value not in choices:
            expected = " or ".join(repr(item) for item in choices)
            self.errors.append(validation_error("literal_error", ["query", name],
                f"Input should be {expected}", value, {"expected": expected}))
        return value

    def check(self) -> None:
        if self.errors:
            raise RequestError(self.errors)


def path_text(request: Request, name: str) -> str:
    return str(cast(object, request.path_params[name]))


class _HttpEndpoint:
    def __init__(self, handler: HttpHandler) -> None:
        self.handler = handler

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive)
        try:
            result = await self.handler(request)
        except RequestError as validation_failure:
            response: Response = JSONResponse({"detail": validation_failure.errors}, status_code=422)
        except HTTPException as http_failure:
            response = JSONResponse({"detail": http_failure.detail}, status_code=http_failure.status_code,
                                    headers=http_failure.headers)
        except ClientDisconnect:
            return
        else:
            response = result if isinstance(result, Response) else JSONResponse(result)
        # File/stream/send failures are outside dispatch exception handling.
        await response(scope, receive, send)


def http_route(path: str, handler: HttpHandler, *, methods: Sequence[str]) -> Route:
    route = Route(path, _HttpEndpoint(handler), methods=list(methods))
    # FastAPI GET routes did not implicitly expose HEAD.
    route.methods = set(methods)
    return route
