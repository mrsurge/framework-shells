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

from framework_shells.supervisor_client import SupervisorClient, SupervisorCursor, SupervisorError
from framework_shells.supervisor_attachment import AttachmentDescriptor, attach_supervisor
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
        self.addCleanup(self.temp.cleanup)
        self.root: Path = Path(self.temp.name)
        self.socket: str = str(self.root / "s")
        self.clients: list[SupervisorClient] = []
        self.child_pid: int | None = None
        config = self.root / "config"
        _ = config.write_text(json.dumps({"socket": self.socket, "stdout_log": str(self.root / "out"),
            "stderr_log": str(self.root / "err"), "command": ["cat"], "cwd": str(self.root),
            "attachment": {"id": "test-shell", "label": "app-worker:example", "spec_id": "example:worker",
                           "subgroups": ["example", "worker"]}}))
        self.process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
            os.environ["FWS_SUPERVISOR_BINARY"], str(config), stdout=asyncio.subprocess.PIPE,
            env={**os.environ, "FRAMEWORK_SHELLS_SECRET": "client-parity"})
        assert self.process.stdout is not None
        async with asyncio.timeout(10):
            ready = cast(dict[str, object], json.loads(await self.process.stdout.readline()))
        self.instance: str = str(ready["instance"])
        self.child_pid = int(str(ready["pid"]))

    async def client(self, backend: Backend | None = None, *, cursor: SupervisorCursor | None = None) -> SupervisorClient:
        client = await SupervisorClient.connect(self.socket, b"client-parity", self.instance,
                                                backend=backend or self.backend, cursor=cursor)
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

    async def test_signed_attachment_replay_is_read_only_until_acquired(self) -> None:
        owner = await self.client()
        _ = await owner.acquire()
        await self.echo(owner, b"observed\x00\xff")
        path = self.root / "s.attachment.json"
        attached = await attach_supervisor(path, b"client-parity", "test-shell",
                                           cursor=SupervisorCursor(), backend=self.backend)
        self.clients.append(attached.client)
        record = attached.descriptor
        self.assertEqual(record.shell.label, "app-worker:example")
        self.assertEqual(record.shell.spec_id, "example:worker")
        self.assertEqual(record.shell.subgroups, ("example", "worker"))
        self.assertEqual(record.cwd, self.root.resolve())
        self.assertEqual(record.supervisor_pid, self.process.pid)
        self.assertEqual(attached.status.pid, self.child_pid)
        self.assertFalse(attached.status.exited)
        self.assertEqual(await self.collect(attached.client, 10, 0), b"observed\x00\xff")
        with self.assertRaises(SupervisorError):
            _ = await attached.client.write(b"no ownership")
        with self.assertRaises(SupervisorError):
            _ = await attached.client.acquire()
        await self.echo(owner, b"owner-still-works")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertNotIn(b"client-parity", path.read_bytes())

    async def test_attachment_rejects_wrong_runtime_shell_and_tampering(self) -> None:
        path = self.root / "s.attachment.json"
        with self.assertRaises(ValueError):
            _ = await attach_supervisor(path, b"wrong-secret", "test-shell", backend=self.backend)
        with self.assertRaisesRegex(ValueError, "shell id"):
            _ = await attach_supervisor(path, b"client-parity", "wrong-shell", backend=self.backend)
        original = path.read_bytes()
        _ = path.write_bytes(original.replace(b"app-worker:example", b"app-worker:changed"))
        with self.assertRaises(ValueError):
            _ = await attach_supervisor(path, b"client-parity", "test-shell", backend=self.backend)
        _ = path.write_bytes(original)
        attached = await attach_supervisor(path, b"client-parity", "test-shell", backend=self.backend)
        self.clients.append(attached.client)
        self.assertEqual(attached.status.pid, self.child_pid)

    async def test_signed_wrong_pid_and_instance_do_not_fake_adoption(self) -> None:
        path = self.root / "s.attachment.json"
        envelope = cast(dict[str, object], json.loads(path.read_bytes()))
        original = cast(dict[str, object], json.loads(str(envelope["body"])))
        for field, value in (("pid", 1), ("instance", "different-instance")):
            body = json.dumps({**original, field: value}, separators=(",", ":"))
            proof = base64.b64encode(hmac.digest(b"client-parity",
                b"fws-supervisor-attachment-v1\0" + body.encode(), "sha256")).decode()
            wrong = self.root / "wrong.attachment.json"
            _ = wrong.write_text(json.dumps({"body": body, "proof": proof}))
            wrong.chmod(0o600)
            with self.assertRaises((ValueError, SupervisorError)):
                _ = await attach_supervisor(wrong, b"client-parity", "test-shell", backend=self.backend)
        attached = await attach_supervisor(path, b"client-parity", "test-shell", backend=self.backend)
        self.clients.append(attached.client)
        self.assertEqual(attached.status.pid, self.child_pid)

    async def test_attachment_retains_exited_identity_but_stale_endpoint_fails(self) -> None:
        client = await self.client()
        _ = await client.acquire()
        await client.eof()
        async with asyncio.timeout(5):
            while (await client.read()).kind != "exited":
                pass
        path = self.root / "s.attachment.json"
        attached = await attach_supervisor(path, b"client-parity", "test-shell", backend=self.backend)
        self.clients.append(attached.client)
        self.assertTrue(attached.status.exited)
        with self.assertRaises(SupervisorError):
            _ = await attached.client.acquire()
        await attached.client.stop()
        _ = await self.process.wait()
        self.assertEqual(AttachmentDescriptor.load(path, b"client-parity").pid, self.child_pid)
        with self.assertRaises(SupervisorError):
            _ = await attach_supervisor(path, b"client-parity", "test-shell", backend=self.backend)

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

    async def collect(self, client: SupervisorClient, count: int, offset: int) -> bytes:
        output = bytearray()
        async with asyncio.timeout(10):
            while len(output) < count:
                event = await client.read()
                self.assertEqual((event.kind, event.stream, event.offset), ("output", "stdout", offset + len(output)))
                output.extend(event.data)
        self.assertEqual(len(output), count)
        return bytes(output)

    async def test_replay_disconnected_bytes_then_live_without_writer_takeover(self) -> None:
        owner = await self.client("python" if self.backend == "native" else ("native" if extension else "python"))
        _ = await owner.acquire()
        prefix, missed, live = b"consumed", b"\x00\xff\x80missed\n", b"live-after-replay"
        await self.echo(owner, prefix)
        await self.echo(owner, missed)
        # Start inside a binary chunk, not on a line/UTF-8 boundary.
        offset = len(prefix) + 1
        resumed = await self.client(cursor=SupervisorCursor(stdout=offset))
        self.assertEqual((await resumed.status()).pid, self.child_pid)
        with self.assertRaises(SupervisorError):
            _ = await resumed.write(b"no-lease")
        self.assertEqual(await self.collect(resumed, len(missed) - 1, offset), missed[1:])
        await self.echo(owner, live)
        self.assertEqual(await self.collect(resumed, len(live), len(prefix + missed)), live)
        self.assertEqual((self.root / "out").read_bytes(), prefix + missed + live)

    async def test_output_during_replay_has_no_duplicate_or_gap(self) -> None:
        owner = await self.client()
        _ = await owner.acquire()
        seed = bytes(range(256)) * 128
        for _ in range(4):
            await self.echo(owner, seed)
        async def burst() -> bytes:
            chunks: list[bytes] = []
            for index in range(24):
                data = bytes([index]) * 1024
                await self.echo(owner, data)
                chunks.append(data)
            return b"".join(chunks)
        writer = asyncio.create_task(burst())
        try:
            resumed = await self.client(cursor=SupervisorCursor())
            expected = seed * 4 + await writer
            self.assertEqual(await self.collect(resumed, len(expected), 0), expected)
            await self.echo(owner, b"after-boundary")
            self.assertEqual(await self.collect(resumed, 14, len(expected)), b"after-boundary")
        finally:
            if not writer.done():
                writer.cancel()
                with suppress(asyncio.CancelledError):
                    await writer

    async def test_replay_rejects_future_and_oversized_cursors_without_clipping(self) -> None:
        with self.assertRaisesRegex(SupervisorError, "cursor"):
            _ = await self.client(cursor=SupervisorCursor(stdout=1))
        owner = await self.client()
        _ = await owner.acquire()
        data = b"x" * 32768
        for _ in range(32):
            await self.echo(owner, data)
        exact = await self.client(cursor=SupervisorCursor())
        self.assertEqual(await self.collect(exact, 1024 * 1024, 0), data * 32)
        await exact.close()
        await self.echo(owner, b"end")
        with self.assertRaisesRegex(SupervisorError, "1048576"):
            _ = await self.client(cursor=SupervisorCursor())
        size = 1024 * 1024 + 3
        resumed = await self.client(cursor=SupervisorCursor(stdout=size - 8))
        self.assertEqual(await self.collect(resumed, 8, size - 8), b"xxxxxend")
        self.assertEqual((self.root / "out").stat().st_size, size)

    async def test_empty_and_at_tail_replay_preserve_next_live_offset(self) -> None:
        empty = await self.client(cursor=SupervisorCursor())
        _ = await empty.acquire()
        await self.echo(empty, b"abc")
        await empty.close()
        tail = await self.client(cursor=SupervisorCursor(stdout=3))
        _ = await tail.acquire(takeover=True)
        _ = await tail.write(b"def")
        self.assertEqual(await self.collect(tail, 3, 3), b"def")

    async def test_replay_rejects_replaced_raw_log(self) -> None:
        owner = await self.client()
        _ = await owner.acquire()
        await self.echo(owner, b"original")
        _ = (self.root / "out").rename(self.root / "old")
        _ = (self.root / "out").write_bytes(b"replaced")
        with self.assertRaisesRegex(SupervisorError, "replaced"):
            _ = await self.client(cursor=SupervisorCursor())
        self.assertEqual((self.root / "old").read_bytes(), b"original")

    async def test_replay_after_child_exit_reports_history_and_exit(self) -> None:
        owner = await self.client()
        _ = await owner.acquire()
        await self.echo(owner, b"before-exit")
        await owner.eof()
        async with asyncio.timeout(5):
            while (await owner.read()).kind != "exited":
                pass
        await owner.close()
        resumed = await self.client(cursor=SupervisorCursor())
        self.assertEqual(await self.collect(resumed, 11, 0), b"before-exit")
        self.assertEqual((await resumed.read()).kind, "exited")
        self.assertTrue((await resumed.status()).exited)

    async def test_replay_preserves_independent_stdout_stderr_cursors(self) -> None:
        config = self.root / "duplex-config"
        socket = str(self.root / "duplex-socket")
        program = ("import sys\nfor line in sys.stdin.buffer:\n"
                   " sys.stdout.buffer.write(line); sys.stdout.buffer.flush()\n"
                   " sys.stderr.buffer.write(line[::-1]); sys.stderr.buffer.flush()\n")
        _ = config.write_text(json.dumps({"socket": socket, "stdout_log": str(self.root / "duplex-out"),
            "stderr_log": str(self.root / "duplex-err"), "command": [sys.executable, "-u", "-c", program]}))
        process = await asyncio.create_subprocess_exec(os.environ["FWS_SUPERVISOR_BINARY"], str(config),
            stdout=asyncio.subprocess.PIPE, env={**os.environ, "FRAMEWORK_SHELLS_SECRET": "client-parity"})
        peers: list[SupervisorClient] = []
        pid: int | None = None
        try:
            assert process.stdout is not None
            async with asyncio.timeout(10):
                ready = cast(dict[str, object], json.loads(await process.stdout.readline()))
                pid = int(str(ready["pid"]))
                instance = str(ready["instance"])
                first = await SupervisorClient.connect(socket, b"client-parity", instance, backend=self.backend)
                peers.append(first)
                _ = await first.acquire()
                data = b"raw\x00\xff\n"
                _ = await first.write(data)

                async def collect_pair(client: SupervisorClient, cursor: SupervisorCursor) -> dict[str, bytes]:
                    buffers = {"stdout": bytearray(), "stderr": bytearray()}
                    starts = {"stdout": cursor.stdout, "stderr": cursor.stderr}
                    while any(len(buffers[key]) < len(data) - starts[key] for key in buffers):
                        event = await client.read()
                        self.assertEqual(event.kind, "output")
                        self.assertEqual(event.offset, starts[event.stream] + len(buffers[event.stream]))
                        buffers[event.stream].extend(event.data)
                    return {key: bytes(value) for key, value in buffers.items()}

                self.assertEqual(await collect_pair(first, SupervisorCursor()), {"stdout": data, "stderr": data[::-1]})
                await first.close()
                cursor = SupervisorCursor(stdout=2, stderr=3)
                resumed = await SupervisorClient.connect(socket, b"client-parity", instance,
                    backend=self.backend, cursor=cursor)
                peers.append(resumed)
                self.assertEqual(await collect_pair(resumed, cursor), {"stdout": data[2:], "stderr": data[::-1][3:]})
                self.assertEqual((self.root / "duplex-out").read_bytes(), data)
                self.assertEqual((self.root / "duplex-err").read_bytes(), data[::-1])
                await resumed.stop()
                _ = await process.wait()
        finally:
            for peer in peers:
                await peer.close()
            if process.returncode is None:
                process.kill()
                _ = await process.wait()
                if pid is not None:
                    with suppress(ProcessLookupError):
                        os.killpg(pid, signal.SIGKILL)

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
