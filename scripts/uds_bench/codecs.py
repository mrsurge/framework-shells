"""Benchmark-only generated codecs. Never imported by the FWS runtime."""
from __future__ import annotations

import base64
from contextlib import AbstractContextManager
from dataclasses import dataclass
import importlib
import json
import os
from pathlib import Path
import sys
from typing import Protocol, cast

ROOT = Path(__file__).resolve().parents[2]
CODECS = ("raw", "json", "json-msgspec", "flatbuffers", "capnp")


class JsonCodec(Protocol):
    def encode(self, obj: object) -> bytes: ...
    def decode(self, data: bytes) -> object: ...


@dataclass(frozen=True)
class Envelope:
    payload: bytes
    version: int = 1
    generation: int = 0x123456789ABCDEF0
    stream: int = 1
    offset: int = 0xFEDCBA9876543210


class Builder(Protocol):
    def CreateByteVector(self, data: bytes) -> int: ...
    def Finish(self, rootTable: int, file_identifier: bytes) -> None: ...
    def Output(self) -> bytearray: ...


class FlatRuntime(Protocol):
    def Builder(self, initialSize: int) -> Builder: ...


class Table(Protocol):
    def Offset(self, vtableOffset: int) -> int: ...
    def Vector(self, off: int) -> int: ...


class FlatReader(Protocol):
    _tab: Table
    def Version(self) -> int: ...
    def Operation(self) -> int: ...
    def Generation(self) -> int: ...
    def Stream(self) -> int: ...
    def Offset(self) -> int: ...
    def PayloadLength(self) -> int: ...
    def PayloadIsNone(self) -> bool: ...


class FlatRoot(Protocol):
    def GetRootAs(self, buf: bytes, offset: int = 0) -> FlatReader: ...
    def EnvelopeBufferHasIdentifier(self, buf: bytes, offset: int) -> bool: ...


class FlatSchema(Protocol):
    Envelope: FlatRoot
    def EnvelopeStart(self, builder: Builder) -> None: ...
    def EnvelopeAddVersion(self, builder: Builder, value: int) -> None: ...
    def EnvelopeAddGeneration(self, builder: Builder, value: int) -> None: ...
    def EnvelopeAddStream(self, builder: Builder, value: int) -> None: ...
    def EnvelopeAddOffset(self, builder: Builder, value: int) -> None: ...
    def EnvelopeAddPayload(self, builder: Builder, value: int) -> None: ...
    def EnvelopeEnd(self, builder: Builder) -> int: ...


class CapReader(Protocol):
    version: int
    operation: object
    generation: int
    stream: int
    offset: int
    payload: bytes


class CapWriter(Protocol):
    def to_bytes(self) -> bytes: ...


class CapRoot(Protocol):
    def new_message(self, *, version: int, operation: str, generation: int,
                    stream: int, offset: int, payload: bytes) -> CapWriter: ...
    def from_bytes(self, data: bytes, *, traversal_limit_in_words: int,
                   nesting_limit: int) -> AbstractContextManager[CapReader]: ...


class CapSchema(Protocol):
    Envelope: CapRoot


class CapRuntime(Protocol):
    def remove_import_hook(self) -> None: ...
    def load(self, file_name: str) -> CapSchema: ...


class Codec:
    def __init__(self, name: str) -> None:
        if name not in CODECS:
            raise ValueError(f"unknown codec: {name}")
        self.name: str = name
        self.flat: FlatSchema | None = None
        self.flat_runtime: FlatRuntime | None = None
        self.cap: CapSchema | None = None
        self.msgspec: JsonCodec | None = None
        if name == "json-msgspec":
            self.msgspec = cast(JsonCodec, cast(object, importlib.import_module("msgspec.json")))
        if name == "flatbuffers":
            generated = Path(os.environ.get("FWS_BENCH_GENERATED", ROOT / ".external/uds-generated"))
            sys.path.insert(0, str(generated))
            self.flat_runtime = cast(FlatRuntime, cast(object, importlib.import_module("flatbuffers")))
            self.flat = cast(FlatSchema, cast(object, importlib.import_module("FwsBench.Envelope")))
        if name == "capnp":
            runtime = cast(CapRuntime, cast(object, importlib.import_module("capnp")))
            runtime.remove_import_hook()
            self.cap = runtime.load(str(Path(__file__).parent / "schemas/envelope.capnp"))

    def encode(self, envelope: Envelope) -> bytes:
        if self.name == "raw":
            return envelope.payload
        if self.name in ("json", "json-msgspec"):
            obj = {"version": envelope.version, "operation": "data",
                "generation": envelope.generation, "stream": envelope.stream,
                "offset": envelope.offset, "payload": base64.b64encode(envelope.payload).decode("ascii")}
            if self.msgspec is not None:
                return self.msgspec.encode(obj)
            return json.dumps(obj, separators=(",", ":")).encode()
        if self.flat is not None and self.flat_runtime is not None:
            builder = self.flat_runtime.Builder(len(envelope.payload) + 128)
            payload = builder.CreateByteVector(envelope.payload)
            self.flat.EnvelopeStart(builder)
            self.flat.EnvelopeAddVersion(builder, envelope.version)
            self.flat.EnvelopeAddGeneration(builder, envelope.generation)
            self.flat.EnvelopeAddStream(builder, envelope.stream)
            self.flat.EnvelopeAddOffset(builder, envelope.offset)
            self.flat.EnvelopeAddPayload(builder, payload)
            builder.Finish(self.flat.EnvelopeEnd(builder), file_identifier=b"FWBE")
            return bytes(builder.Output())
        assert self.cap is not None
        return self.cap.Envelope.new_message(version=envelope.version, operation="data",
            generation=envelope.generation, stream=envelope.stream, offset=envelope.offset,
            payload=envelope.payload).to_bytes()

    def decode(self, data: bytes) -> Envelope:
        if self.name == "raw":
            return Envelope(data)
        if self.name in ("json", "json-msgspec"):
            parsed: object = self.msgspec.decode(data) if self.msgspec is not None else json.loads(data)
            if not isinstance(parsed, dict):
                raise ValueError("invalid envelope")
            obj = cast(dict[str, object], parsed)
            if obj.get("operation") != "data":
                raise ValueError("invalid operation")
            numbers: list[int] = []
            for key, maximum in (("version", 65535), ("generation", 2**64 - 1),
                                 ("stream", 255), ("offset", 2**64 - 1)):
                value = obj.get(key)
                if type(value) is not int or not 0 <= value <= maximum:
                    raise ValueError(f"invalid {key}")
                numbers.append(value)
            payload = obj.get("payload")
            if not isinstance(payload, str):
                raise ValueError("invalid payload")
            return Envelope(base64.b64decode(payload, validate=True), *numbers)
        if self.flat is not None:
            if not self.flat.Envelope.EnvelopeBufferHasIdentifier(data, 0):
                raise ValueError("wrong FlatBuffers identifier")
            reader = self.flat.Envelope.GetRootAs(data)
            if reader.Operation() != 0 or reader.PayloadIsNone():
                raise ValueError("invalid envelope")
            # Pinned generated layout: bulk-copy via runtime vector accessor, not a
            # per-byte Python loop or an extra NumPy dependency. Not a full verifier.
            table = reader._tab  # pyright: ignore[reportPrivateUsage]
            start = table.Vector(table.Offset(14))
            end = start + reader.PayloadLength()
            if not 0 <= start <= end <= len(data):
                raise ValueError("payload outside frame")
            return Envelope(data[start:end], reader.Version(), reader.Generation(),
                            reader.Stream(), reader.Offset())
        assert self.cap is not None
        with self.cap.Envelope.from_bytes(data, traversal_limit_in_words=16384, nesting_limit=16) as reader:
            if str(reader.operation) != "data":
                raise ValueError("invalid operation")
            return Envelope(reader.payload, reader.version, reader.generation, reader.stream, reader.offset)
