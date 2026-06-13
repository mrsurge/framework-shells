from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
from asyncio import Queue as AsyncQueue

PTYWriteRequest = tuple[bytes, asyncio.Future[None]]


@dataclass
class PTYState:
    master_fd: int
    label: str | None = None
    shell_id: str | None = None
    backend: str = "pty"
    subscribers: list[AsyncQueue[str]] = field(default_factory=list)
    subscribers_bytes: list[AsyncQueue[bytes]] = field(default_factory=list)
    input_queue: AsyncQueue[PTYWriteRequest | None] = field(default_factory=AsyncQueue)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    reader: asyncio.Task[None] | None = None
    writer: asyncio.Task[None] | None = None
    proxy_pid: int | None = None
    stdout_bytes_seen: int = 0


@dataclass
class PipeState:
    """State for shells with live stdin/stdout pipes (for LSP, etc.)."""
    process: asyncio.subprocess.Process
    label: str | None = None
    shell_id: str | None = None
    stdout_subscribers: list[AsyncQueue[str]] = field(default_factory=list)
    stdout_subscribers_bytes: list[AsyncQueue[bytes]] = field(default_factory=list)
    stderr_subscribers: list[AsyncQueue[str]] = field(default_factory=list)
    stderr_subscribers_bytes: list[AsyncQueue[bytes]] = field(default_factory=list)
    native_initial_chunks: list[bytes] = field(default_factory=list)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    stdout_reader: asyncio.Task[None] | None = None
    stderr_reader: asyncio.Task[None] | None = None
    waiter: asyncio.Task[None] | None = None
    native_engine: str | None = None
    native_phase: str | None = None
    native_pump: object | None = None
    native_reader_fd: int | None = None
    native_chunk_queue: AsyncQueue[bytes | None] | None = None
    stdin_supported: bool = True
    stdout_bytes_seen: int = 0
    stderr_bytes_seen: int = 0
