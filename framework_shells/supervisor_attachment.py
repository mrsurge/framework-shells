"""Experimental manager-facing attachment, not automatic FWS shell adoption.

A signed record is discovery metadata. Authentication plus status proves the
connection; it never authorizes PID signals or acquires the writer lease.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import stat
from typing import Literal, cast

from .supervisor_client import SupervisorClient, SupervisorCursor, SupervisorStatus

MAX_RECORD_BYTES = 64 * 1024
_DOMAIN = b"fws-supervisor-attachment-v1\0"


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate attachment field")
        result[key] = value
    return result


def _mapping(value: object, allowed: set[str]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected attachment object")
    result = cast(dict[str, object], value)
    if not set(result).issubset(allowed):
        raise ValueError("unknown attachment field")
    return result


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("expected attachment string")
    return value


def _optional_text(value: object) -> str | None:
    return None if value is None else _text(value)


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("expected attachment string array")
    return tuple(_text(item) for item in cast(list[object], value))


def _pid(value: object) -> int:
    if type(value) is not int or not 0 < value <= 2**32 - 1:
        raise ValueError("invalid attachment PID")
    return value


@dataclass(frozen=True)
class ShellIdentity:
    id: str
    label: str | None
    spec_id: str | None
    subgroups: tuple[str, ...]


@dataclass(frozen=True)
class AttachmentDescriptor:
    version: int
    runtime_id: str
    shell: ShellIdentity
    instance: str
    pid: int
    supervisor_pid: int
    socket: Path
    stdout_log: Path
    stderr_log: Path
    command: tuple[str, ...]
    cwd: Path

    @classmethod
    def decode(cls, data: bytes, secret: bytes) -> AttachmentDescriptor:
        if not secret or len(data) > MAX_RECORD_BYTES:
            raise ValueError("empty secret or oversized attachment")
        envelope = _mapping(cast(object, json.loads(data.decode("utf-8"), object_pairs_hook=_unique_pairs)), {"body", "proof"})
        body = _text(envelope.get("body")).encode("utf-8")
        proof = base64.b64decode(_text(envelope.get("proof")), validate=True)
        if not hmac.compare_digest(proof, hmac.digest(secret, _DOMAIN + body, "sha256")):
            raise ValueError("attachment authentication failed")
        fields = _mapping(cast(object, json.loads(body, object_pairs_hook=_unique_pairs)), {
            "version", "runtime_id", "shell", "instance", "pid", "supervisor_pid",
            "socket", "stdout_log", "stderr_log", "command", "cwd",
        })
        if type(fields.get("version")) is not int or fields["version"] != 1:
            raise ValueError("unsupported attachment version")
        runtime_id = _text(fields.get("runtime_id"))
        if runtime_id != hashlib.sha256(secret).hexdigest()[:16]:
            raise ValueError("wrong attachment runtime")
        shell = _mapping(fields.get("shell"), {"id", "label", "spec_id", "subgroups"})
        shell_id = _text(shell.get("id"))
        if not shell_id or len(shell_id.encode()) > 256 or any(c in shell_id for c in ("/", "\\", "\0")):
            raise ValueError("invalid shell id")
        identity = ShellIdentity(shell_id, _optional_text(shell.get("label")),
                                 _optional_text(shell.get("spec_id")), _strings(shell.get("subgroups")))
        instance = _text(fields.get("instance"))
        command = _strings(fields.get("command"))
        paths = [Path(_text(fields.get(key))) for key in ("socket", "stdout_log", "stderr_log", "cwd")]
        if not instance or not command or not all(path.is_absolute() for path in paths):
            raise ValueError("invalid attachment instance/command/paths")
        return cls(1, runtime_id, identity, instance, _pid(fields.get("pid")),
                   _pid(fields.get("supervisor_pid")), paths[0], paths[1], paths[2], command, paths[3])

    @classmethod
    def load(cls, path: Path, secret: bytes) -> AttachmentDescriptor:
        parent = path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077 or parent.st_uid != os.geteuid():
            raise ValueError("attachment directory must be private and owned")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(fd, "rb") as file:
            metadata = os.fstat(file.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077 or metadata.st_uid != os.geteuid():
                raise ValueError("attachment must be a private owned regular file")
            record = cls.decode(file.read(MAX_RECORD_BYTES + 1), secret)
        if record.socket.parent != path.parent:
            raise ValueError("attachment/socket directory mismatch")
        return record


@dataclass(frozen=True)
class SupervisorAttachment:
    descriptor: AttachmentDescriptor
    client: SupervisorClient
    # Authenticated state at attachment, not a permanently-live assertion.
    status: SupervisorStatus


async def attach_supervisor(path: Path, secret: bytes, expected_shell_id: str, *,
                            cursor: SupervisorCursor | None = None,
                            backend: Literal["auto", "python", "native"] = "auto") -> SupervisorAttachment:
    """Observe only. No launch, PID signaling, implicit takeover, or write retry."""
    descriptor = await asyncio.to_thread(AttachmentDescriptor.load, path, secret)
    if descriptor.shell.id != expected_shell_id:
        raise ValueError("wrong attachment shell id")
    client = await SupervisorClient.connect(str(descriptor.socket), secret, descriptor.instance,
                                            cursor=cursor, backend=backend)
    try:
        status = await client.status()
        if status.pid != descriptor.pid or status.instance != descriptor.instance:
            raise ValueError("attachment status mismatch")
        return SupervisorAttachment(descriptor, client, status)
    except BaseException:
        await client.close()
        raise
