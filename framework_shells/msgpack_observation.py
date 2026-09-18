"""Bounded single-frame MessagePack decoding for observation, never pipe routing."""
from __future__ import annotations

import math
import struct
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Protocol, cast

from .log_projection import JsonValue, PARSE_BYTES


@dataclass(frozen=True)
class _Pairs:
    items: list[tuple[object, object]]


@dataclass(frozen=True)
class _Extension:
    code: int
    data: bytes


class _Timestamp(Protocol):
    seconds: int
    nanoseconds: int


class _Unpacker(Protocol):
    def feed(self, data: bytes | memoryview) -> None: ...
    def unpack(self) -> object: ...
    def tell(self) -> int: ...


class _Factory(Protocol):
    def __call__(self, *, raw: bool, strict_map_key: bool, max_buffer_size: int,
                 object_pairs_hook: Callable[[list[tuple[object, object]]], _Pairs],
                 ext_hook: Callable[[int, bytes], _Extension]) -> _Unpacker: ...


class _Msgpack(Protocol):
    Unpacker: _Factory
    OutOfData: type[Exception]
    Timestamp: type[_Timestamp]


_msgpack = cast(_Msgpack, cast(object, import_module("msgpack")))


def _normalize(value: object, depth: int = 0) -> JsonValue:
    if depth > 64:
        raise ValueError("MessagePack nesting limit exceeded")
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value if abs(value) <= 9007199254740991 else {"$fws": "integer", "decimal": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            return {"$fws": "float64", "hex": struct.pack(">d", value).hex()}
        return value
    if isinstance(value, bytes):
        return {"$fws": "binary", "hex": value.hex()}
    if isinstance(value, _Extension):
        return {"$fws": "extension", "code": value.code, "hex": value.data.hex()}
    if isinstance(value, _msgpack.Timestamp):
        return {"$fws": "timestamp", "seconds": str(value.seconds), "nanoseconds": value.nanoseconds}
    if isinstance(value, list):
        return [_normalize(item, depth + 1) for item in cast(list[object], value)]
    if isinstance(value, _Pairs):
        keys = [key for key, _ in value.items]
        if all(isinstance(key, str) for key in keys):
            strings = cast(list[str], keys)
            if "$fws" not in strings and len(set(strings)) == len(strings):
                return {cast(str, key): _normalize(item, depth + 1) for key, item in value.items}
        return {"$fws": "map", "entries": [
            [_normalize(key, depth + 1), _normalize(item, depth + 1)]
            for key, item in value.items
        ]}
    raise ValueError("unsupported MessagePack value")


@dataclass(frozen=True)
class DecodedFrame:
    value: JsonValue
    consumed: int


def decode_frame(data: bytes | memoryview, max_bytes: int = PARSE_BYTES) -> DecodedFrame | None:
    """Decode the first concatenated object; None means incomplete, errors are terminal.

    Callers retain incomplete bytes and must not resynchronize by guessing.
    Feed at most max_bytes bytes, so declared lengths cannot grow a stream buffer
    without bound. The frame decoder does not accumulate an unbounded queue.
    """
    if not 1 <= max_bytes <= PARSE_BYTES:
        raise ValueError("invalid MessagePack budget")
    unpacker = _msgpack.Unpacker(raw=False, strict_map_key=False,
        max_buffer_size=max_bytes, object_pairs_hook=_Pairs, ext_hook=_Extension)
    unpacker.feed(data[:max_bytes])
    try:
        value = unpacker.unpack()
    except _msgpack.OutOfData:
        if len(data) >= max_bytes:
            raise ValueError("MessagePack frame exceeds byte budget") from None
        return None
    except Exception as exc:
        raise ValueError("invalid MessagePack frame") from exc
    return DecodedFrame(_normalize(value), unpacker.tell())
