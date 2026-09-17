"""Synchronous indexed reads; async hosts must run these on their I/O executor."""
from __future__ import annotations

import json
import os
import struct
import tempfile
import threading
import uuid
import weakref
from collections import OrderedDict
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .log_projection import Codec, PARSE_BYTES, RECORD_BYTES, RawReference, RecordProjection, project_record, WindowAction
from .msgpack_observation import decode_frame

SCAN_BYTES = 65536
RESPONSE_BYTES = 1024 * 1024


def reset_token(path: Path) -> bytes:
    try:
        with path.with_name(path.name + ".fws-reset").open("rb") as source:
            return source.read(64)
    except FileNotFoundError:
        return b""


def mark_log_reset(path: Path) -> None:
    marker = path.with_name(path.name + ".fws-reset")
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as writer:
        temporary = Path(writer.name)
        try:
            _ = writer.write(uuid.uuid4().hex.encode("ascii"))
            writer.close()
            temporary.replace(marker)
        finally:
            temporary.unlink(missing_ok=True)


class IndexCache:
    """Bounded index ownership; borrow only inside an I/O-executor operation.

    The lock covers the borrow so eviction cannot close an in-use index.
    Source paths must come from authorized shell records, not client input.
    """

    def __init__(self, capacity: int = 32) -> None:
        if capacity < 1:
            raise ValueError("index capacity must be positive")
        self._capacity = capacity
        self._entries: OrderedDict[tuple[Path, Codec], LineIndex] = OrderedDict()
        self._finalizer = weakref.finalize(self, self._close_entries, self._entries)
        self._lock = threading.RLock()
        self._closed = False

    @contextmanager
    def borrow(self, path: Path, codec: Codec = "text") -> Iterator[LineIndex]:
        with self._lock:
            if self._closed:
                raise RuntimeError("index cache is closed")
            key = (path.absolute(), codec)
            index = self._entries.get(key)
            if index is None:
                if len(self._entries) >= self._capacity:
                    _, oldest = self._entries.popitem(last=False)
                    oldest.close()
                index = LineIndex(key[0], codec)
                self._entries[key] = index
            self._entries.move_to_end(key)
            yield index

    def invalidate(self, path: Path) -> None:
        with self._lock:
            absolute = path.absolute()
            for (source, _), index in self._entries.items():
                if source == absolute:
                    index.invalidate()

    def close(self) -> None:
        with self._lock:
            self._finalizer()
            self._closed = True

    @staticmethod
    def _close_entries(entries: OrderedDict[tuple[Path, Codec], LineIndex]) -> None:
        for index in entries.values():
            index.close()
        entries.clear()


@dataclass(frozen=True)
class LogWindow:
    generation: str
    start: int
    end: int
    total: int
    at_start: bool
    at_tail: bool
    records: list[RecordProjection]
    pending_bytes: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "generation": self.generation, "start": self.start, "end": self.end,
            "total": self.total, "at_start": self.at_start, "at_tail": self.at_tail,
            "records": [record.to_dict() for record in self.records],
            "pending_bytes": self.pending_bytes,
        }


def serialized_window(window: LogWindow) -> bytes:
    return json.dumps(window.to_dict(), ensure_ascii=False, separators=(",", ":")).encode()


class LineIndex:
    """Disk-backed newline offsets with bounded scan and record-read buffers.

    Each instance owns a temporary index, never the source log. Append-only
    writers may run concurrently. Explicit reset must invalidate the index.
    """

    def __init__(self, path: Path, codec: Codec = "text") -> None:
        if codec not in ("text", "json", "messagepack"):
            raise ValueError("invalid codec")
        self.path = path
        self.codec: Codec = codec
        self._pending = b""
        self._index = tempfile.TemporaryFile(mode="w+b")
        self._lock = threading.RLock()
        self._identity: tuple[int, int] | None = None
        self._mtime: int = 0
        self._reset = b""
        self._size = 0
        self._complete = 0
        self._last_end = 0
        self._generation = uuid.uuid4().hex

    def close(self) -> None:
        with self._lock:
            self._index.close()

    def invalidate(self) -> None:
        with self._lock:
            self._identity = None

    def _refresh(self) -> None:
        try:
            self._refresh_inner()
        except Exception:
            self._identity = None
            raise

    def _refresh_inner(self) -> None:
        with self.path.open("rb") as source:
            stat = os.fstat(source.fileno())
            identity = (stat.st_dev, stat.st_ino)
            token = reset_token(self.path)
            if (self._reset != token or self._identity != identity or stat.st_size < self._size
                    or (stat.st_size == self._size and stat.st_mtime_ns != self._mtime)):
                _ = self._index.seek(0)
                _ = self._index.truncate()
                self._size = self._complete = self._last_end = 0
                self._pending = b""
                self._generation = uuid.uuid4().hex
            self._identity = identity
            self._reset = token
            _ = source.seek(self._size)
            _ = self._index.seek(0, os.SEEK_END)
            while self._size < stat.st_size:
                data = source.read(min(SCAN_BYTES, stat.st_size - self._size))
                if not data:
                    raise RuntimeError("log changed during indexing")
                if self.codec == "messagepack":
                    self._pending += data
                    consumed = 0
                    pending_view = memoryview(self._pending)
                    while consumed < len(self._pending):
                        frame = decode_frame(pending_view[consumed:])
                        if frame is None:
                            break
                        consumed += frame.consumed
                        self._last_end += frame.consumed
                        _ = self._index.write(struct.pack("<Q", self._last_end))
                        self._complete += 1
                    self._pending = self._pending[consumed:]
                else:
                    position = data.find(b"\n")
                    while position >= 0:
                        self._last_end = self._size + position + 1
                        _ = self._index.write(struct.pack("<Q", self._last_end))
                        self._complete += 1
                        position = data.find(b"\n", position + 1)
                self._size += len(data)
            self._mtime = stat.st_mtime_ns
            self._validate_snapshot()

    def _validate_snapshot(self) -> None:
        stat = self.path.stat()
        if reset_token(self.path) != self._reset or (stat.st_dev, stat.st_ino) != self._identity or stat.st_size < self._size:
            raise RuntimeError("log generation changed")
        if stat.st_size == self._size and stat.st_mtime_ns != self._mtime:
            raise RuntimeError("log changed during read")

    def _offset(self, ordinal: int) -> int:
        if ordinal == 0:
            return 0
        if ordinal > self._complete:
            return self._size
        _ = self._index.seek((ordinal - 1) * 8)
        return int(struct.unpack("<Q", self._index.read(8))[0])

    def _read(self, start: int, count: int) -> bytes:
        if reset_token(self.path) != self._reset:
            raise RuntimeError("log generation changed")
        with self.path.open("rb") as source:
            stat = os.fstat(source.fileno())
            if (stat.st_dev, stat.st_ino) != self._identity or stat.st_size < self._size:
                raise RuntimeError("log generation changed")
            if stat.st_size == self._size and stat.st_mtime_ns != self._mtime:
                raise RuntimeError("log changed during read")
            _ = source.seek(start)
            result = source.read(count)
            if len(result) != count:
                raise RuntimeError("log changed during read")
            self._validate_snapshot()
            return result

    def raw(self, reference: RawReference, offset: int = 0, limit: int = SCAN_BYTES) -> bytes:
        with self._lock:
            if offset < 0 or not 1 <= limit <= SCAN_BYTES:
                raise ValueError("invalid raw read bounds")
            self._refresh()
            if reference.generation != self._generation:
                raise ValueError("stale generation")
            if not 0 <= reference.byte_start <= reference.byte_end <= self._size:
                raise ValueError("invalid raw reference")
            start = min(reference.byte_end, reference.byte_start + offset)
            return self._read(start, min(limit, reference.byte_end - start))

    def window(
        self, *, action: WindowAction = "tail", current: int = 0,
        count: int = 200, shift: int = 50, codec: Codec | None = None,
        max_bytes: int = RESPONSE_BYTES, generation: str | None = None,
    ) -> LogWindow:
        with self._lock:
            if not 1 <= count <= 1000 or not 512 <= max_bytes <= RESPONSE_BYTES:
                raise ValueError("invalid window budget")
            codec = self.codec if codec is None else codec
            if (codec == "messagepack") != (self.codec == "messagepack"):
                raise ValueError("codec does not match index framing")
            self._refresh()
            if generation is not None and generation != self._generation:
                raise ValueError("stale generation")
            pending = self._size - self._last_end
            total = self._complete + int(pending > 0 and self.codec != "messagepack")
            if current < 0 or shift < 1 or action not in ("tail", "older", "newer", "current"):
                raise ValueError("invalid window navigation")
            reverse = action in ("tail", "older")
            boundary = total if action == "tail" else min(current, total)
            length = min(count, shift) if action in ("older", "newer") else count
            start = max(0, boundary - length) if reverse else boundary
            stop = boundary if reverse else min(total, start + length)
            records: list[RecordProjection] = []
            # Tail reads must keep the newest records when bytes limit the window.
            indices = range(stop - 1, start - 1, -1) if reverse else range(start, stop)
            actual_start = boundary
            end = boundary
            for ordinal in indices:
                byte_start, byte_end = self._offset(ordinal), self._offset(ordinal + 1)
                raw = RawReference(self._generation, byte_start, byte_end)
                if byte_end - byte_start > PARSE_BYTES:
                    data = self._read(byte_start, RECORD_BYTES + 4)
                    preview = data.decode("utf-8", errors="replace").encode()[:RECORD_BYTES].decode("utf-8", errors="ignore")
                    record = RecordProjection(preview, raw, [], "parse_budget_exceeded")
                else:
                    record = project_record(self._read(byte_start, byte_end - byte_start), raw, codec=codec)
                candidate = [record, *records] if reverse else [*records, record]
                low = ordinal if reverse else start
                high = stop if reverse else ordinal + 1
                proposed = LogWindow(self._generation, low, high, total, low == 0, high == total, candidate, pending)
                if len(serialized_window(proposed)) > max_bytes:
                    if not records:
                        raise ValueError("window budget cannot fit first record; increase max_bytes")
                    break
                records, actual_start, end = candidate, low, high
            self._validate_snapshot()
            return LogWindow(self._generation, actual_start, end, total, actual_start == 0, end == total, records, pending)
