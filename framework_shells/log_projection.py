"""Transport-independent log projection primitives.

Offsets always address the original bytes. Display omissions never modify the
source, and an omitted projection must not be interpreted as a protocol packet.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal, TypeAlias, cast

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
Codec: TypeAlias = Literal["text", "json", "messagepack"]
WindowAction: TypeAlias = Literal["tail", "older", "newer", "current"]
RECORD_BYTES = 8192
PARSE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class RawReference:
    generation: str
    byte_start: int
    byte_end: int


@dataclass(frozen=True)
class Omission:
    pointer: str
    original_type: str
    serialized_bytes: int


@dataclass(frozen=True)
class RecordProjection:
    text: str
    raw: RawReference
    omissions: list[Omission]
    diagnostic: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "text": self.text,
            "raw": asdict(self.raw),
            "omissions": [asdict(item) for item in self.omissions],
            "diagnostic": self.diagnostic,
        }


def window_start(total: int, current: int, count: int, shift: int, action: WindowAction) -> int:
    if total < 0 or current < 0 or count < 1 or shift < 1 or shift > count:
        raise ValueError("invalid window bounds")
    maximum = max(0, total - count)
    start = min(current, maximum)
    if action == "tail":
        return maximum
    if action == "older":
        return max(0, start - shift)
    if action == "newer":
        return min(maximum, start + shift)
    if action == "current":
        return start
    raise ValueError("invalid window action")


def _encoded(value: JsonValue) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")


def _kind(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "number"


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _candidates(value: dict[str, JsonValue], path: str = "") -> list[tuple[int, str, str, dict[str, JsonValue], str]]:
    result: list[tuple[int, str, str, dict[str, JsonValue], str]] = []
    for key, item in value.items():
        if (not path and key in {"jsonrpc", "id", "method"}) or (path == "/error" and key == "code"):
            continue
        pointer = path + "/" + key.replace("~", "~0").replace("/", "~1")
        if isinstance(item, dict) and item:
            result.extend(_candidates(item, pointer))
        else:
            result.append((len(_encoded(item)), pointer, _kind(item), value, key))
    return result


def project_record(
    data: bytes,
    raw: RawReference,
    *,
    codec: Codec = "text",
    max_bytes: int = RECORD_BYTES,
    parse_bytes: int = PARSE_BYTES,
) -> RecordProjection:
    if max_bytes < 32 or parse_bytes < max_bytes:
        raise ValueError("invalid projection budgets")
    if raw.byte_start < 0 or raw.byte_end - raw.byte_start != len(data):
        raise ValueError("raw reference does not match record bytes")
    if codec not in ("text", "json", "messagepack"):
        raise ValueError("invalid codec")
    if codec == "messagepack":
        raise ValueError("MessagePack requires the frame decoder")
    # Limit preview decoding before attempting any structured parsing.
    text = data[:max_bytes].decode("utf-8", errors="replace")
    if len(data) <= max_bytes and len(text.encode("utf-8")) <= max_bytes:
        return RecordProjection(text, raw, [], None)

    def preview(reason: str) -> RecordProjection:
        # Decode a few extra bytes so a boundary inside valid UTF-8 is not
        # mistaken for invalid source data before applying the output budget.
        prefix = data[:max_bytes + 4].decode("utf-8", errors="replace")
        return RecordProjection(prefix.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore"), raw, [], reason)

    if codec == "text":
        return preview("record_too_large")
    if len(data) > parse_bytes:
        return preview("parse_budget_exceeded")
    try:
        value = cast(JsonValue, json.loads(data.decode("utf-8"), parse_constant=_reject_constant))
        compact = _encoded(value)
    except (ValueError, UnicodeError, RecursionError):
        return preview("invalid_json")
    if len(compact) <= max_bytes:
        return RecordProjection(compact.decode("utf-8"), raw, [], None)
    if not isinstance(value, dict):
        return preview("structured_summary_required")

    # Removing nested values preserves surrounding routing and diagnostic context.
    try:
        candidates = sorted(_candidates(value), key=lambda item: (-item[0], item[1]))
    except RecursionError:
        return preview("structured_summary_required")
    omissions: list[Omission] = []
    for size, pointer, kind, parent, key in candidates:
        del parent[key]
        omissions.append(Omission(pointer, kind, size))
        compact = _encoded(value)
        if len(compact) <= max_bytes:
            return RecordProjection(compact.decode("utf-8"), raw, omissions, "values_omitted")
    return preview("structured_summary_required")
