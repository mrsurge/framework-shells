from __future__ import annotations

import asyncio
import fnmatch
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles


JSON_FORMAT = "json"
JSONRPC_FORMAT = "jsonrpc"
PLAIN_FORMAT = "plain"
PLAIN_PREFIX_RE = re.compile(r"^\[([A-Za-z0-9._:-]+)\](?:\s|$)")


def _strip_line_endings(text: str) -> str:
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith("\n") or text.endswith("\r"):
        return text[:-1]
    return text


def _safe_json_summary(value: Any) -> str:
    if isinstance(value, dict):
        keys = list(value.keys())[:5]
        suffix = ", ..." if len(value) > 5 else ""
        return f"object keys={keys}{suffix}"
    if isinstance(value, list):
        return f"array len={len(value)}"
    return repr(value)[:120]


def _extract_plain_prefix(text: str) -> Optional[str]:
    match = PLAIN_PREFIX_RE.match(str(text or ""))
    if not match:
        return None
    prefix = str(match.group(1) or "").strip()
    return prefix or None


def _classify_jsonrpc(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"formats": [], "kinds": [], "signature": None}

    has_version = payload.get("jsonrpc") == "2.0" or "jsonrpc" in payload
    has_method = "method" in payload and payload.get("method") is not None
    has_id = "id" in payload
    has_params = "params" in payload
    has_result = "result" in payload
    has_error = "error" in payload

    if has_error and not (has_version or has_id):
        return {"formats": [], "kinds": [], "signature": None}
    if has_error:
        return {
            "formats": [JSONRPC_FORMAT],
            "kinds": ["jsonrpc:error"],
            "signature": "jsonrpc:error",
        }
    if has_result and not (has_version or has_id):
        return {"formats": [], "kinds": [], "signature": None}
    if has_result:
        return {
            "formats": [JSONRPC_FORMAT],
            "kinds": ["jsonrpc:response"],
            "signature": "jsonrpc:result",
        }
    if has_method and not (has_version or has_id or has_params):
        return {"formats": [], "kinds": [], "signature": None}
    if has_method and has_id:
        method = str(payload.get("method"))
        return {
            "formats": [JSONRPC_FORMAT],
            "kinds": ["jsonrpc:request"],
            "signature": f"jsonrpc:method={method}",
        }
    if has_method:
        method = str(payload.get("method"))
        return {
            "formats": [JSONRPC_FORMAT],
            "kinds": ["jsonrpc:notification"],
            "signature": f"jsonrpc:method={method}",
        }
    return {"formats": [], "kinds": [], "signature": None}


def _extract_json_fragments(text: str) -> List[Dict[str, Any]]:
    fragments: List[Dict[str, Any]] = []
    start: Optional[int] = None
    stack: List[str] = []
    in_string = False
    escaped = False

    for idx, ch in enumerate(text):
        if start is None:
            if ch in "{[":
                start = idx
                stack = [ch]
                in_string = False
                escaped = False
            continue

        if in_string:
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch in "{[":
            stack.append(ch)
            continue
        if ch in "}]":
            if not stack:
                start = None
                continue
            opener = stack[-1]
            if (opener == "{" and ch != "}") or (opener == "[" and ch != "]"):
                start = None
                stack = []
                continue
            stack.pop()
            if stack:
                continue
            raw = text[start : idx + 1]
            try:
                parsed = json.loads(raw)
            except Exception:
                start = None
                continue
            fragments.append(
                {
                    "format": JSON_FORMAT,
                    "start": start,
                    "end": idx + 1,
                    "summary": _safe_json_summary(parsed),
                    "parsed": parsed,
                }
            )
            start = None
            stack = []

    return fragments


def _build_inspected_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(raw_record)
    text = str(record.get("text") or "")
    prefix = _extract_plain_prefix(text)
    fragments = _extract_json_fragments(text)
    formats_detected: List[str] = []
    kinds: List[str] = []
    json_payloads: List[Any] = []
    signature: Optional[str] = None

    for frag in fragments:
        parsed = frag.get("parsed")
        json_payloads.append(parsed)
        if JSON_FORMAT not in formats_detected:
            formats_detected.append(JSON_FORMAT)

        jsonrpc = _classify_jsonrpc(parsed)
        for fmt in jsonrpc["formats"]:
            if fmt not in formats_detected:
                formats_detected.append(fmt)
        for kind in jsonrpc["kinds"]:
            if kind not in kinds:
                kinds.append(kind)
        if signature is None and jsonrpc["signature"]:
            signature = str(jsonrpc["signature"])

    if not formats_detected:
        formats_detected = [PLAIN_FORMAT]
    if signature is None and prefix:
        signature = f"{PLAIN_FORMAT}:{prefix}"
    if not kinds:
        kinds = [signature or formats_detected[0]]
    if signature is None:
        signature = formats_detected[0]

    record.update(
        {
            "text_truncated": False,
            "prefix": prefix,
            "formats_detected": formats_detected,
            "kinds": kinds,
            "event_signature": signature,
            "fragments": fragments,
            "json_payloads": json_payloads,
        }
    )
    return record


def _record_matches(
    record: Dict[str, Any],
    *,
    query: Optional[str],
    exclude_query: Optional[str],
    regex: bool,
    ignore_case: bool,
    format_filter: Optional[str],
    signature_filter: Optional[str],
    exclude_signature: Optional[str],
    compiled_query: Optional[re.Pattern[str]] = None,
    compiled_exclude_query: Optional[re.Pattern[str]] = None,
) -> bool:
    text = str(record.get("text") or "")
    if query:
        if regex:
            if compiled_query is None or not compiled_query.search(text):
                return False
        else:
            haystack = text.lower() if ignore_case else text
            needle = str(query).lower() if ignore_case else str(query)
            if needle not in haystack:
                return False

    if exclude_query:
        if regex:
            if compiled_exclude_query is not None and compiled_exclude_query.search(text):
                return False
        else:
            haystack = text.lower() if ignore_case else text
            needle = str(exclude_query).lower() if ignore_case else str(exclude_query)
            if needle in haystack:
                return False

    if format_filter:
        formats = record.get("formats_detected") or []
        if str(format_filter) not in formats:
            return False

    if signature_filter:
        event_signature = str(record.get("event_signature") or "")
        if not fnmatch.fnmatch(event_signature, str(signature_filter)):
            return False

    if exclude_signature:
        event_signature = str(record.get("event_signature") or "")
        if fnmatch.fnmatch(event_signature, str(exclude_signature)):
            return False

    return True


def summarize_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts_by_signature: Dict[str, int] = {}
    counts_by_kind: Dict[str, int] = {}
    counts_by_format: Dict[str, int] = {}

    for record in records:
        signature = str(record.get("event_signature") or "")
        if signature:
            counts_by_signature[signature] = counts_by_signature.get(signature, 0) + 1
        for kind in record.get("kinds") or []:
            key = str(kind)
            counts_by_kind[key] = counts_by_kind.get(key, 0) + 1
        for fmt in record.get("formats_detected") or []:
            key = str(fmt)
            counts_by_format[key] = counts_by_format.get(key, 0) + 1

    top_signatures = sorted(
        counts_by_signature.items(),
        key=lambda item: (-item[1], item[0]),
    )
    return {
        "counts_by_signature": counts_by_signature,
        "counts_by_kind": counts_by_kind,
        "counts_by_format": counts_by_format,
        "top_signatures": [
            {"signature": signature, "count": count}
            for signature, count in top_signatures[:10]
        ],
    }


async def read_event_window(path: Path, *, lines: int, max_bytes: int) -> Dict[str, Any]:
    if lines <= 0 or not path.exists():
        return {
            "records": [],
            "byte_window_start": 0,
            "byte_window_end": 0,
            "partial_head": False,
            "truncated": False,
            "event_count": 0,
        }

    stat = await asyncio.to_thread(path.stat)
    file_size = int(stat.st_size)
    if file_size <= 0:
        return {
            "records": [],
            "byte_window_start": 0,
            "byte_window_end": 0,
            "partial_head": False,
            "truncated": False,
            "event_count": 0,
        }

    to_read = min(file_size, max(0, int(max_bytes)))
    start_offset = max(0, file_size - to_read)
    previous_byte = b""
    if start_offset > 0:
        async with aiofiles.open(path, "rb") as fh:
            await fh.seek(start_offset - 1)
            previous_byte = await fh.read(1)

    async with aiofiles.open(path, "rb") as fh:
        await fh.seek(start_offset)
        data = await fh.read(to_read)

    if not data:
        return {
            "records": [],
            "byte_window_start": start_offset,
            "byte_window_end": start_offset,
            "partial_head": False,
            "truncated": start_offset > 0,
            "event_count": 0,
        }

    raw_lines = data.splitlines(keepends=True)
    if data and not raw_lines:
        raw_lines = [data]

    starts_mid_line = start_offset > 0 and previous_byte not in {b"", b"\n", b"\r"}
    records: List[Dict[str, Any]] = []
    cursor = start_offset
    for idx, raw_line in enumerate(raw_lines):
        decoded = raw_line.decode("utf-8", errors="replace")
        text = _strip_line_endings(decoded)
        record = {
            "stream": None,
            "ordinal": idx + 1,
            "line_number": None,
            "byte_start": cursor,
            "byte_end": cursor + len(raw_line),
            "partial_head": bool(idx == 0 and starts_mid_line),
            "partial_tail": not raw_line.endswith((b"\n", b"\r")),
            "raw_length": len(raw_line),
            "text": text,
        }
        cursor += len(raw_line)
        records.append(record)

    selected = records[-lines:] if lines > 0 else []
    if not selected:
        return {
            "records": [],
            "byte_window_start": start_offset,
            "byte_window_end": start_offset + len(data),
            "partial_head": False,
            "truncated": start_offset > 0,
            "event_count": 0,
        }

    for idx, record in enumerate(selected):
        record["ordinal"] = idx + 1

    return {
        "records": selected,
        "byte_window_start": int(selected[0]["byte_start"]),
        "byte_window_end": int(selected[-1]["byte_end"]),
        "partial_head": bool(selected[0].get("partial_head")),
        "truncated": start_offset > 0,
        "event_count": len(selected),
    }


async def inspect_log_file(
    path: Path,
    *,
    stream: str,
    lines: int,
    max_bytes: int,
    query: Optional[str] = None,
    exclude_query: Optional[str] = None,
    regex: bool = False,
    ignore_case: bool = False,
    format_filter: Optional[str] = None,
    signature_filter: Optional[str] = None,
    exclude_signature: Optional[str] = None,
) -> Dict[str, Any]:
    window = await read_event_window(path, lines=lines, max_bytes=max_bytes)
    inspected: List[Dict[str, Any]] = []
    compiled_query: Optional[re.Pattern[str]] = None
    compiled_exclude_query: Optional[re.Pattern[str]] = None
    if query and regex:
        flags = re.IGNORECASE if ignore_case else 0
        try:
            compiled_query = re.compile(str(query), flags)
        except re.error as exc:
            raise ValueError(f"Invalid regex: {query}") from exc
    if exclude_query and regex:
        flags = re.IGNORECASE if ignore_case else 0
        try:
            compiled_exclude_query = re.compile(str(exclude_query), flags)
        except re.error as exc:
            raise ValueError(f"Invalid exclude regex: {exclude_query}") from exc

    for raw_record in window["records"]:
        raw_record["stream"] = stream
        record = _build_inspected_record(raw_record)
        if not _record_matches(
            record,
            query=query,
            exclude_query=exclude_query,
            regex=regex,
            ignore_case=ignore_case,
            format_filter=format_filter,
            signature_filter=signature_filter,
            exclude_signature=exclude_signature,
            compiled_query=compiled_query,
            compiled_exclude_query=compiled_exclude_query,
        ):
            continue
        inspected.append(record)

    return {
        "records": inspected,
        "summary": summarize_records(inspected),
        "byte_window_start": window["byte_window_start"],
        "byte_window_end": window["byte_window_end"],
        "partial_head": window["partial_head"],
        "truncated": window["truncated"],
        "event_count": window["event_count"],
    }
