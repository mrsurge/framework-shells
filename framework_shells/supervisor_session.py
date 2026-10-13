"""Experimental authenticated UDS session; not wired into the FWS manager."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import secrets
from typing import cast

from .supervisor_transport import read_frame, write_frame

Message = dict[str, object]


def _string(message: Message, key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str):
        raise ValueError(f"invalid {key}")
    return value


async def receive_message(reader: asyncio.StreamReader) -> Message:
    frame = await read_frame(reader)
    if frame is None:
        raise ConnectionError("supervisor disconnected")
    value = cast(object, json.loads(frame))
    if not isinstance(value, dict):
        raise ValueError("expected supervisor object")
    return cast(Message, value)


async def send_message(writer: asyncio.StreamWriter, message: Message) -> None:
    async with asyncio.timeout(3):
        await write_frame(writer, json.dumps(message, separators=(",", ":")).encode())


async def connect(path: str, secret: bytes, instance: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Authenticate a new connection; failures close it. No implicit input retries."""
    if not secret:
        raise ValueError("empty secret")
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(3):
            reader, writer = await asyncio.open_unix_connection(path)
            challenge = await receive_message(reader)
            if type(challenge.get("version")) is not int or challenge["version"] != 1:
                raise ValueError("unsupported supervisor version")
            if _string(challenge, "instance") != instance:
                raise ValueError("wrong supervisor instance")
            nonce = _string(challenge, "nonce")
            if len(base64.b64decode(nonce, validate=True)) != 32:
                raise ValueError("invalid nonce")
            client_nonce = base64.b64encode(secrets.token_bytes(32)).decode()

            def proof(role: str) -> str:
                message = b"\0".join(part.encode() for part in ("fws-supervisor-v1", role, instance, nonce, client_nonce))
                return base64.b64encode(hmac.digest(secret, message, hashlib.sha256)).decode()

            await send_message(writer, {"version": 1, "instance": instance, "nonce": client_nonce, "proof": proof("client")})
            reply = await receive_message(reader)
            if reply.get("version") != 1 or reply.get("instance") != instance or reply.get("nonce") != client_nonce:
                raise ValueError("server session mismatch")
            if not hmac.compare_digest(_string(reply, "proof"), proof("server")):
                raise ValueError("server authentication failed")
            return reader, writer
    except BaseException:
        if writer is not None:
            writer.close()
            try:
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            except (OSError, TimeoutError):
                pass
        raise
