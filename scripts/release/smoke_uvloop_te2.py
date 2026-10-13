"""Run with the isolated wheel-install interpreter, never the source tree."""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
from pathlib import Path
import socket
import sys
import tempfile
from collections.abc import Callable, Coroutine
from typing import cast
import importlib


async def smoke() -> None:
    loop = asyncio.get_running_loop()
    assert type(loop).__module__.startswith("uvloop"), type(loop)
    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.setblocking(False)
    ready: asyncio.Future[bytes] = loop.create_future()

    def readable() -> None:
        chunk = reader.recv(1024)
        loop.remove_reader(reader.fileno())
        ready.set_result(chunk)

    try:
        loop.add_reader(reader.fileno(), readable)
        writer.sendall(b"fd-ready\x00\xff")
        assert await asyncio.wait_for(ready, 5) == b"fd-ready\x00\xff"
    finally:
        loop.remove_reader(reader.fileno())
        reader.close()
        writer.close()

    payload = bytes(range(256)) * 64
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-I", "-c",
        "import sys; data=sys.stdin.buffer.read(); "
        "sys.stdout.buffer.write(data); sys.stderr.buffer.write(b'stderr-ok')",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(payload), 10)
        assert stdout == payload and stderr == b"stderr-ok" and proc.returncode == 0
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()

    complete: asyncio.Future[None] = loop.create_future()

    async def echo(stream: asyncio.StreamReader, output: asyncio.StreamWriter) -> None:
        try:
            data = await stream.readexactly(len(payload))
            output.write(data)
            await output.drain()
            complete.set_result(None)
        except Exception as error:
            complete.set_exception(error)
        finally:
            output.close()
            await output.wait_closed()

    with tempfile.TemporaryDirectory(prefix="uvloop-te2-") as directory:
        path = str(Path(directory) / "echo.sock")
        server = await asyncio.start_unix_server(echo, path)
        async with server:
            stream, output = await asyncio.open_unix_connection(path)
            try:
                output.write(payload)
                await output.drain()
                assert await asyncio.wait_for(stream.readexactly(len(payload)), 5) == payload
                await asyncio.wait_for(complete, 5)
            finally:
                output.close()
                await output.wait_closed()

    idle = asyncio.create_task(asyncio.sleep(60))
    idle.cancel()
    try:
        await idle
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("Cancelled task completed normally")
    print("PASS: uvloop selection, fd readiness, binary subprocess/EOF, UDS, cancellation")


def main() -> None:
    distribution = importlib.metadata.distribution("uvloop-te2")
    assert distribution.version == "0.23.0.post1"
    try:
        importlib.metadata.distribution("uvloop")
    except importlib.metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("Validation venv also contains upstream uvloop")
    module = importlib.import_module("uvloop")
    runner = cast(Callable[[Coroutine[object, object, None]], None], module.run)
    print("distribution", distribution.version, "module", module.__file__)
    print("interpreter", sys.executable, "platform", sys.platform)
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(3):
        runner(smoke())
    # libuv can retain a process-wide signal pipe after first initialization.
    after_warmup = len(os.listdir("/proc/self/fd"))
    for _ in range(3):
        runner(smoke())
    assert len(os.listdir("/proc/self/fd")) == after_warmup
    print("PASS: repeated loop close has stable fd count", before, after_warmup)


if __name__ == "__main__":
    main()
