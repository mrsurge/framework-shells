from dataclasses import dataclass, field
import asyncio
from asyncio import Queue as AsyncQueue

@dataclass
class PTYState:
    master_fd: int
    label: str | None = None
    shell_id: str | None = None
    subscribers: list[AsyncQueue[str]] = field(default_factory=list)
    subscribers_bytes: list[AsyncQueue[bytes]] = field(default_factory=list)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    reader: asyncio.Task[None] | None = None
    proxy_pid: int | None = None


@dataclass
class PipeState:
    """State for shells with live stdin/stdout pipes (for LSP, etc.)."""
    process: asyncio.subprocess.Process
    label: str | None = None
    shell_id: str | None = None
    stdout_subscribers: list[AsyncQueue[str]] = field(default_factory=list)
    stdout_subscribers_bytes: list[AsyncQueue[bytes]] = field(default_factory=list)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    stdout_reader: asyncio.Task[None] | None = None
    waiter: asyncio.Task[None] | None = None
