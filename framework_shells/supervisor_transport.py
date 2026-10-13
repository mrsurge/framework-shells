"""Bounded supervisor framing only; not an authenticated session or pipe adapter."""
from __future__ import annotations

import asyncio
import struct

MAX_FRAME_BYTES = 64 * 1024


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """None means clean socket EOF, never child stdin EOF."""
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise
    size = int.from_bytes(header, "big")
    if size > MAX_FRAME_BYTES:
        raise ValueError("supervisor frame exceeds 65536 bytes")
    return await reader.readexactly(size)


async def write_frame(writer: asyncio.StreamWriter, payload: bytes) -> None:
    """Caller serializes writes and handles partial/cancelled delivery as uncertain."""
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("supervisor frame exceeds 65536 bytes")
    writer.write(struct.pack("!I", len(payload)) + payload)
    await writer.drain()
