#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from typing import Any


def _coerce_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except Exception:
            return default
    return default


def _payload(size: int, *, fill: str) -> str:
    if size <= 0:
        return ""
    return (fill * ((size // len(fill)) + 1))[:size]


def _write_message(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")


def _handle_request(request: dict[str, Any]) -> None:
    request_id = request.get("id")
    method = str(request.get("method") or "")
    params_obj = request.get("params")
    params = params_obj if isinstance(params_obj, dict) else {}

    if method != "bench.echo":
        _write_message(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"unknown method: {method}"},
            }
        )
        sys.stdout.flush()
        return

    response_bytes = max(0, _coerce_int(params.get("response_bytes"), 0))
    push_count = max(0, _coerce_int(params.get("push_count"), 0))
    push_bytes = max(0, _coerce_int(params.get("push_bytes"), 0))
    ordinal = max(0, _coerce_int(params.get("ordinal"), 0))

    for index in range(push_count):
        _write_message(
            {
                "jsonrpc": "2.0",
                "method": "bench.push",
                "params": {
                    "request_id": request_id,
                    "ordinal": ordinal,
                    "index": index,
                    "payload": _payload(push_bytes, fill="push_"),
                },
            }
        )

    _write_message(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "ok": True,
                "ordinal": ordinal,
                "payload": _payload(response_bytes, fill="resp_"),
            },
        }
    )
    sys.stdout.flush()


def main() -> int:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except Exception:
            _write_message(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error"},
                }
            )
            sys.stdout.flush()
            continue
        if not isinstance(request, dict):
            _write_message(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "invalid request"},
                }
            )
            sys.stdout.flush()
            continue
        _handle_request(request)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
