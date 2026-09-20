"""Same client contract against real isolated supervisors, never live managers."""
from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
import importlib.util
import json
import hmac
import os
from pathlib import Path
import signal
import sys
import tempfile
from typing import Literal, cast
import unittest
from unittest.mock import patch

from framework_shells.supervisor_client import SupervisorClient, SupervisorError
from framework_shells.supervisor_session import receive_message, send_message

extension = os.environ.get("FWS_SUPERVISOR_CLIENT_EXTENSION")
if extension:
    spec = importlib.util.spec_from_file_location("fws_supervisor_client", extension)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["fws_supervisor_client"] = module

Backend = Literal["python", "native"]


@unittest.skipUnless(os.environ.get("FWS_SUPERVISOR_BINARY"), "supervisor binary required")
class PythonClientTests(unittest.IsolatedAsyncioTestCase):
    backend: Backend = "python"

    async def asyncSetUp(self) -> None:
        self.temp: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(prefix="fwsclient-")
        self.root: Path = Path(self.temp.name)
        self.socket: str = str(self.root / "s")
        self.clients: list[SupervisorClient] = []
        self.child_pid: int | None = None
        config = self.root / "config"
        _ = config.write_text(json.dumps({"socket": self.socket, "stdout_log": str(self.root / "out"),
            "stderr_log": str(self.root / "err"), "command": ["cat"]}))
        self.process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
            os.environ["FWS_SUPERVISOR_BINARY"], str(config), stdout=asyncio.subprocess.PIPE,
            env={**os.environ, "FRAMEWORK_SHELLS_SECRET": "client-parity"})
        assert self.process.stdout is not None
        async with asyncio.timeout(10):
            ready = cast(dict[str, object], json.loads(await self.process.stdout.readline()))
        self.instance: str = str(ready["instance"])
        self.child_pid = int(str(ready["pid"]))

    async def client(self, backend: Backend | None = None) -> SupervisorClient:
        client = await SupervisorClient.connect(self.socket, b"client-parity", self.instance,
                                                backend=backend or self.backend)
        self.clients.append(client)
        return client

    async def asyncTearDown(self) -> None:
        try:
            if self.process.returncode is None:
                client = await self.client("python")
                await client.stop()
                async with asyncio.timeout(5):
                    _ = await self.process.wait()
        finally:
            for client in self.clients:
                await client.close()
            if self.process.returncode is None:
                self.process.kill()
                _ = await self.process.wait()
                if self.child_pid is not None:
                    with suppress(ProcessLookupError):
                        os.killpg(self.child_pid, signal.SIGKILL)
            self.temp.cleanup()

    async def echo(self, client: SupervisorClient, data: bytes) -> None:
        self.assertEqual(await client.write(data), len(data))
        output = bytearray()
        async with asyncio.timeout(5):
            while len(output) < len(data):
                event = await client.read()
                self.assertEqual((event.kind, event.stream), ("output", "stdout"))
                output.extend(event.data)
        self.assertEqual(bytes(output), data)

    async def test_binary_reconnect_takeover_and_raw_logs(self) -> None:
        first = await self.client()
        _ = await first.acquire()
        await self.echo(first, b"\x00\xff\n")
        second = await self.client()
        with self.assertRaises(SupervisorError) as rejected:
            _ = await second.acquire()
        self.assertEqual(rejected.exception.kind, "remote")
        _ = await second.acquire(True)
        with self.assertRaises(SupervisorError):
            _ = await first.write(b"must-not-write")
        await first.close()
        await self.echo(second, bytes(range(256)) * 128)
        await second.close()
        third = await self.client()
        _ = await third.acquire(True)
        self.assertEqual((await third.status()).pid, self.child_pid)
        self.assertFalse((await third.status()).exited)
        await self.echo(third, b"after-disconnect")
        self.assertEqual((self.root / "out").read_bytes(), b"\x00\xff\n" + bytes(range(256)) * 128 + b"after-disconnect")

    async def test_auth_and_preconnection_selection(self) -> None:
        with self.assertRaises(SupervisorError) as wrong:
            _ = await SupervisorClient.connect(self.socket, b"wrong", self.instance, backend=self.backend)
        self.assertEqual(wrong.exception.kind, "connect")
        with self.assertRaises(SupervisorError):
            _ = await SupervisorClient.connect(self.socket, b"client-parity", "wrong", backend=self.backend)
        with patch("framework_shells.supervisor_client._native", return_value=None):
            fallback = await SupervisorClient.connect(self.socket, b"client-parity", self.instance)
            self.clients.append(fallback)
            self.assertEqual(fallback.backend, "python")
            with self.assertRaises(SupervisorError) as missing:
                _ = await SupervisorClient.connect(self.socket, b"client-parity", self.instance, backend="native")
            self.assertEqual(missing.exception.kind, "unavailable")
        if extension:
            automatic = await SupervisorClient.connect(self.socket, b"client-parity", self.instance)
            self.clients.append(automatic)
            self.assertEqual(automatic.backend, "native")
            with patch("framework_shells.supervisor_client._PythonClient.connect") as fallback_call:
                with self.assertRaises(SupervisorError):
                    _ = await SupervisorClient.connect(self.socket, b"wrong", self.instance)
                fallback_call.assert_not_called()

    async def test_eof_detach_and_rejected_writes(self) -> None:
        client = await self.client()
        with self.assertRaises(SupervisorError) as no_lease:
            _ = await client.write(b"no-lease")
        self.assertEqual(no_lease.exception.kind, "invalid")
        _ = await client.acquire()
        with self.assertRaises(SupervisorError) as oversized:
            _ = await client.write(b"x" * 32769)
        self.assertEqual(oversized.exception.kind, "invalid")
        await client.detach()
        _ = await client.acquire()
        await self.echo(client, b"before-eof")
        await client.eof()
        with self.assertRaises(SupervisorError):
            _ = await client.write(b"after-eof")
        async with asyncio.timeout(5):
            while (await client.read()).kind != "exited":
                pass
        self.assertTrue((await client.status()).exited)
        self.assertEqual((self.root / "out").read_bytes(), b"before-eof")

    async def test_cancel_idle_reader_does_not_close_or_lose_next_bytes(self) -> None:
        client = await self.client()
        _ = await client.acquire()
        read = asyncio.create_task(client.read())
        await asyncio.sleep(0.02)  # Let the foreign future register before cancellation.
        read.cancel()
        with self.assertRaises(asyncio.CancelledError):
            _ = await read
        await self.echo(client, b"still-open")

    async def test_close_wakes_idle_reader(self) -> None:
        client = await self.client()
        task = asyncio.create_task(client.read())
        await client.close()
        async with asyncio.timeout(5):
            with self.assertRaises(SupervisorError):
                _ = await task

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "procfs FD accounting required")
    async def test_repeated_close_releases_descriptors(self) -> None:
        first = await self.client()
        await first.close()  # Initialize shared runtime before measuring.
        baseline = len(os.listdir("/proc/self/fd"))
        for _ in range(8):
            client = await self.client()
            _ = await client.status()
            await client.close()
        self.assertLessEqual(len(os.listdir("/proc/self/fd")), baseline)

    async def test_lost_ack_and_overflow_fail_closed(self) -> None:
        for overflow in (False, True):
            completed = asyncio.get_running_loop().create_future()
            writes: list[str] = []
            async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                try:
                    nonce = base64.b64encode(bytes(32)).decode()
                    await send_message(writer, {"version": 1, "instance": "fault", "nonce": nonce})
                    proof = await receive_message(reader)
                    client_nonce = str(proof["nonce"])
                    text = b"\0".join(p.encode() for p in ("fws-supervisor-v1", "server", "fault", nonce, client_nonce))
                    mac = base64.b64encode(hmac.digest(b"client-parity", text, "sha256")).decode()
                    await send_message(writer, {"version": 1, "instance": "fault", "nonce": client_nonce, "proof": mac})
                    await send_message(writer, {"event": "attached", "instance": "fault"})
                    if overflow:
                        # Exceed all bounded native delivery stages, not only
                        # the reference client's single 256-event queue.
                        for offset in range(1500):
                            await send_message(writer, {"event": "output", "stream": "stdout", "offset": offset, "data": "eA=="})
                        _ = await reader.read()  # Keep connection alive until the consumer rejects it.
                    else:
                        self.assertEqual((await receive_message(reader))["op"], "acquire")
                        await send_message(writer, {"ok": True, "result": {"generation": 1}})
                        writes.append(str((await receive_message(reader))["op"]))
                        # No acknowledgement. Delivery may already have occurred.
                except (ConnectionError, BrokenPipeError):
                    pass
                except Exception as exc:
                    completed.set_exception(exc)
                finally:
                    writer.close()
                    with suppress(OSError):
                        await writer.wait_closed()
                    if not completed.done():
                        completed.set_result(None)

            path = str(self.root / ("overflow" if overflow else "lost-ack"))
            listener = await asyncio.start_unix_server(serve, path)
            async with listener, asyncio.timeout(10):
                client = await SupervisorClient.connect(path, b"client-parity", "fault", backend=self.backend)
                self.clients.append(client)
                if overflow:
                    await completed
                    with self.assertRaises(SupervisorError) as failed:
                        _ = await client.status()
                    self.assertEqual(failed.exception.kind, "lagged")
                    with self.assertRaises(SupervisorError) as failed_read:
                        _ = await client.read()
                    self.assertEqual(failed_read.exception.kind, "lagged")
                else:
                    _ = await client.acquire()
                    with self.assertRaises(SupervisorError) as failed_write:
                        _ = await client.write(b"one")
                    self.assertEqual(failed_write.exception.kind, "delivery_uncertain")
                    with self.assertRaises(SupervisorError):
                        _ = await client.write(b"never-retry")
                    await completed
                    self.assertEqual(writes, ["write"])
                await client.close()


@unittest.skipUnless(extension, "native client extension required")
class NativeClientTests(PythonClientTests):
    backend: Backend = "native"
