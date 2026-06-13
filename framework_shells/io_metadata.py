from __future__ import annotations

import asyncio
import hashlib
import json
import time
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict, cast

import aiofiles


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonMap: TypeAlias = dict[str, JsonValue]
IoMetadataKind = Literal["output", "stdin_write", "stdin_eof"]
IoMetadataStream = Literal["stdout", "stderr", "stdin"]
StdinCaptureMode = Literal["none", "preview", "full"]

SCHEMA = "framework-shells.io_metadata.v1"
DEFAULT_STDIN_PREVIEW_BYTES = 240
MAX_STDIN_PREVIEW_BYTES = 4096
DEFAULT_READ_MAX_BYTES = 2 * 1024 * 1024


class IoMetadataRecord(TypedDict, total=False):
    schema: str
    shell_id: str
    kind: IoMetadataKind
    stream: IoMetadataStream
    ts: float
    source: str
    backend: str
    byte_start: int
    byte_end: int
    byte_count: int
    append_newline: bool
    newline_appended: bool
    preview: str
    text: str
    preview_truncated: bool
    sha256: str


def io_metadata_path(logs_dir: Path, shell_id: str) -> Path:
    return logs_dir / f"{shell_id}.io_metadata.jsonl"


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def io_metadata_enabled(debug: object) -> bool:
    if not isinstance(debug, dict):
        return False
    debug_map = cast(dict[object, object], debug)
    return _truthy(debug_map.get("io_metadata")) or _truthy(debug_map.get("ioMetadata"))


def stdin_capture_mode(debug: object) -> StdinCaptureMode:
    if not isinstance(debug, dict):
        return "preview"
    debug_map = cast(dict[object, object], debug)
    raw = str(debug_map.get("stdin_capture") or debug_map.get("stdinCapture") or "preview").strip().lower()
    if raw in {"none", "off", "false", "0"}:
        return "none"
    if raw == "full":
        return "full"
    return "preview"


def stdin_preview_bytes(debug: object) -> int:
    if not isinstance(debug, dict):
        return DEFAULT_STDIN_PREVIEW_BYTES
    debug_map = cast(dict[object, object], debug)
    raw = debug_map.get("stdin_preview_bytes")
    if raw is None:
        raw = debug_map.get("stdinPreviewBytes")
    if isinstance(raw, bool) or raw is None:
        return DEFAULT_STDIN_PREVIEW_BYTES
    if not isinstance(raw, (str, int, float)):
        return DEFAULT_STDIN_PREVIEW_BYTES
    try:
        parsed = int(raw)
    except Exception:
        return DEFAULT_STDIN_PREVIEW_BYTES
    return max(0, min(parsed, MAX_STDIN_PREVIEW_BYTES))


def build_output_record(
    *,
    shell_id: str,
    stream: Literal["stdout", "stderr"],
    byte_start: int,
    byte_end: int,
    ts: float | None = None,
) -> IoMetadataRecord:
    byte_count = max(0, int(byte_end) - int(byte_start))
    return {
        "schema": SCHEMA,
        "shell_id": shell_id,
        "kind": "output",
        "stream": stream,
        "ts": time.time() if ts is None else float(ts),
        "byte_start": int(byte_start),
        "byte_end": int(byte_end),
        "byte_count": byte_count,
    }


def build_stdin_write_record(
    *,
    shell_id: str,
    source: str,
    backend: str,
    payload: str,
    append_newline: bool,
    debug: object,
    ts: float | None = None,
) -> IoMetadataRecord:
    encoded = payload.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    capture_mode = stdin_capture_mode(debug)
    preview_limit = stdin_preview_bytes(debug)
    record: IoMetadataRecord = {
        "schema": SCHEMA,
        "shell_id": shell_id,
        "kind": "stdin_write",
        "stream": "stdin",
        "ts": time.time() if ts is None else float(ts),
        "source": str(source or "unknown"),
        "backend": backend,
        "byte_count": len(encoded),
        "append_newline": bool(append_newline),
        "newline_appended": bool(append_newline),
        "sha256": digest,
    }
    if capture_mode == "full":
        record["text"] = payload
        record["preview"] = payload
        record["preview_truncated"] = False
    elif capture_mode == "preview" and preview_limit > 0:
        preview_bytes = encoded[:preview_limit]
        record["preview"] = preview_bytes.decode("utf-8", errors="replace")
        record["preview_truncated"] = len(encoded) > len(preview_bytes)
    else:
        record["preview_truncated"] = len(encoded) > 0
    return record


def build_stdin_eof_record(
    *,
    shell_id: str,
    source: str,
    backend: str,
    ts: float | None = None,
) -> IoMetadataRecord:
    return {
        "schema": SCHEMA,
        "shell_id": shell_id,
        "kind": "stdin_eof",
        "stream": "stdin",
        "ts": time.time() if ts is None else float(ts),
        "source": str(source or "unknown"),
        "backend": backend,
        "byte_count": 0,
    }


async def append_io_metadata(path: Path, record: IoMetadataRecord) -> None:
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    line = json.dumps(cast(JsonMap, dict(record)), ensure_ascii=False, separators=(",", ":")) + "\n"
    async with aiofiles.open(path, "a", encoding="utf-8") as fh:
        await fh.write(line)


def _decode_record(raw: bytes) -> IoMetadataRecord | None:
    try:
        parsed = cast(object, json.loads(raw.decode("utf-8", errors="replace")))
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    record = cast(dict[object, object], parsed)
    kind = record.get("kind")
    stream = record.get("stream")
    shell_id = record.get("shell_id")
    if kind not in {"output", "stdin_write", "stdin_eof"}:
        return None
    if stream not in {"stdout", "stderr", "stdin"}:
        return None
    if not isinstance(shell_id, str):
        return None
    return cast(IoMetadataRecord, dict(record))


async def read_io_metadata(
    path: Path,
    *,
    limit: int = 1000,
    max_bytes: int = DEFAULT_READ_MAX_BYTES,
    include_output: bool = True,
    include_stdin: bool = True,
    include_timestamps: bool = True,
) -> list[IoMetadataRecord]:
    if limit <= 0 or not path.exists():
        return []

    stat = await asyncio.to_thread(path.stat)
    file_size = int(stat.st_size)
    if file_size <= 0:
        return []

    to_read = min(file_size, max(0, int(max_bytes)))
    start_offset = max(0, file_size - to_read)
    async with aiofiles.open(path, "rb") as fh:
        await fh.seek(start_offset)
        data = await fh.read(to_read)

    records: list[IoMetadataRecord] = []
    for raw_line in data.splitlines():
        decoded = _decode_record(raw_line)
        if decoded is None:
            continue
        kind = decoded.get("kind")
        if kind == "output" and not include_output:
            continue
        if kind in {"stdin_write", "stdin_eof"} and not include_stdin:
            continue
        if not include_timestamps:
            decoded.pop("ts", None)
        records.append(decoded)
    return records[-limit:]
