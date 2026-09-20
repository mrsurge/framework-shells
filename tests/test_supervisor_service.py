"""Disposable process acceptance for the experimental Rust supervisor, not FWS launch."""
from __future__ import annotations

import asyncio
import base64
import json
import os
import signal
import sys
import tempfile
import unittest
from collections import deque
from pathlib import Path
from typing import cast

from framework_shells.supervisor_session import Message, connect, receive_message, send_message


class Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader: asyncio.StreamReader = reader
        self.writer: asyncio.StreamWriter = writer
        self.events: deque[Message] = deque(maxlen=128)

    async def request(self, message: Message) -> Message:
        await send_message(self.writer, message)
        async with asyncio.timeout(5):
            while True:
                reply = await receive_message(self.reader)
                if "ok" in reply:
                    return reply
                self.events.append(reply)

    async def output(self) -> bytes:
        async with asyncio.timeout(5):
            while True:
                reply = self.events.popleft() if self.events else await receive_message(self.reader)
                if reply.get("event") == "output" and reply.get("stream") == "stdout":
                    return base64.b64decode(str(reply["data"]), validate=True)

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


def result(reply: Message) -> Message:
    if reply.get("ok") is not True:
        raise AssertionError(reply)
    return cast(Message, reply["result"])


@unittest.skipUnless(os.environ.get("FWS_SUPERVISOR_BINARY"), "build/set experimental supervisor binary")
class SupervisorServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(prefix="fws-")
        self.root: Path = Path(self.directory.name)
        self.socket: Path = self.root / "s"
        self.clients: list[Client] = []
        self.secret: bytes = b"disposable-supervisor-secret"
        config = self.root / "config.json"
        child = (
            "import sys,uuid,json; nonce=uuid.uuid4().hex; count=0\n"
            "for line in sys.stdin.buffer:\n"
            " count+=1; print(json.dumps({'nonce':nonce,'count':count}),flush=True); "
            "sys.stderr.buffer.write(line); sys.stderr.buffer.flush()\n"
        )
        _ = config.write_text(json.dumps({"socket": str(self.socket), "stdout_log": str(self.root / "out"),
            "stderr_log": str(self.root / "err"), "command": [sys.executable, "-u", "-c", child]}))
        self.process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
            os.environ["FWS_SUPERVISOR_BINARY"], str(config), stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env={**os.environ, "FRAMEWORK_SHELLS_SECRET": self.secret.decode()},
        )
        self.child_pid: int | None = None
        try:
            assert self.process.stdout is not None
            async with asyncio.timeout(10):
                line = await self.process.stdout.readline()
            if not line:
                assert self.process.stderr is not None
                raise AssertionError((await self.process.stderr.read()).decode())
            ready = cast(Message, json.loads(line))
            self.instance: str = str(ready["instance"])
            self.child_pid = int(str(ready["pid"]))
            self.assertEqual(os.getsid(self.process.pid), self.process.pid)
            self.assertEqual(os.getsid(self.child_pid), self.process.pid)
            self.assertEqual(os.getpgid(self.child_pid), self.child_pid)
        except BaseException:
            await self.asyncTearDown()
            raise

    async def client(self) -> Client:
        reader, writer = await connect(str(self.socket), self.secret, self.instance)
        attached = await receive_message(reader)
        self.assertEqual(attached["event"], "attached")
        client = Client(reader, writer)
        self.clients.append(client)
        return client

    async def asyncTearDown(self) -> None:
        try:
            if self.process.returncode is None and self.child_pid is not None:
                client = await self.client()
                _ = await client.request({"op": "stop"})
                async with asyncio.timeout(5):
                    _ = await self.process.wait()
        finally:
            for client in self.clients:
                try:
                    await client.close()
                except OSError:
                    pass
            if self.process.returncode is None:
                self.process.kill()
                _ = await self.process.wait()
                if self.child_pid is not None:
                    try:
                        os.killpg(self.child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            self.directory.cleanup()

    async def test_authenticated_reconnect_preserves_child_and_fences_old_writer(self) -> None:
        first = await self.client()
        generation = result(await first.request({"op": "acquire"}))["generation"]
        payload = base64.b64encode(b"first\x00\xff\n").decode()
        _ = result(await first.request({"op": "write", "generation": generation, "data": payload}))
        before = cast(Message, json.loads(await first.output()))
        second = await self.client()
        self.assertFalse((await second.request({"op": "acquire"}))["ok"])
        new = result(await second.request({"op": "acquire", "takeover": True}))["generation"]
        self.assertFalse((await first.request({"op": "write", "generation": generation, "data": payload}))["ok"])
        self.assertFalse((await first.request({"op": "eof", "generation": generation}))["ok"])
        await first.close()
        _ = result(await second.request({"op": "write", "generation": new, "data": payload}))
        after = cast(Message, json.loads(await second.output()))
        self.assertEqual((before["nonce"], before["count"]), (after["nonce"], 1))
        self.assertEqual(after["count"], 2)
        await second.close()  # Disconnect alone must not deliver EOF.
        third = await self.client()
        current = result(await third.request({"op": "acquire", "takeover": True}))["generation"]
        status = result(await third.request({"op": "status"}))
        self.assertEqual(status["pid"], self.child_pid)
        self.assertFalse(status["exited"])
        _ = result(await third.request({"op": "write", "generation": current, "data": payload}))
        final = cast(Message, json.loads(await third.output()))
        self.assertEqual(final, {"nonce": before["nonce"], "count": 3})
        _ = result(await third.request({"op": "stop"}))
        async with asyncio.timeout(5):
            self.assertEqual(await self.process.wait(), 0)
        self.assertFalse(self.socket.exists())
        self.assertEqual((self.root / "err").read_bytes(), b"first\x00\xff\n" * 3)
        self.assertEqual(len((self.root / "out").read_bytes().splitlines()), 3)

    async def test_wrong_secret_instance_and_eof(self) -> None:
        with self.assertRaises((ConnectionError, asyncio.IncompleteReadError)):
            _ = await connect(str(self.socket), b"wrong", self.instance)
        with self.assertRaisesRegex(ValueError, "instance"):
            _ = await connect(str(self.socket), self.secret, "wrong")
        reader, writer = await asyncio.open_unix_connection(str(self.socket))
        try:
            _ = await receive_message(reader)
            await send_message(writer, {"version": 2, "instance": self.instance, "nonce": "bad", "proof": "bad"})
            async with asyncio.timeout(5):
                with self.assertRaises(ConnectionError):
                    _ = await receive_message(reader)
        finally:
            writer.close()
            await writer.wait_closed()
        client = await self.client()
        generation = result(await client.request({"op": "acquire"}))["generation"]
        _ = result(await client.request({"op": "eof", "generation": generation}))
        self.assertFalse((await client.request({"op": "acquire", "takeover": True}))["ok"])
        # Exit notification is event-driven; no status polling.
        async with asyncio.timeout(5):
            while True:
                event = client.events.popleft() if client.events else await receive_message(client.reader)
                if event.get("event") == "exited":
                    break
        self.assertEqual(event["code"], 0)
        resumed = await self.client()
        self.assertTrue(result(await resumed.request({"op": "status"}))["exited"])
        self.assertFalse((await resumed.request({"op": "acquire"}))["ok"])

    async def test_abrupt_client_exit_does_not_restart_child(self) -> None:
        client = await self.client()
        generation = result(await client.request({"op": "acquire"}))["generation"]
        _ = result(await client.request({"op": "write", "generation": generation, "data": "eAo="}))
        before = cast(Message, json.loads(await client.output()))
        _ = result(await client.request({"op": "detach", "generation": generation}))
        probe = (
            "import asyncio,os,sys\n"
            "from framework_shells.supervisor_session import connect,receive_message,send_message\n"
            "async def main():\n"
            " r,w=await connect(sys.argv[1],os.environ['FRAMEWORK_SHELLS_SECRET'].encode(),sys.argv[2])\n"
            " await receive_message(r)\n"
            " await send_message(w,{'op':'acquire'})\n"
            " reply=await receive_message(r)\n"
            " assert reply['ok'], reply\n"
            " os._exit(0)\n"
            "asyncio.run(main())\n"
        )
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", probe, str(self.socket), self.instance,
            env={**os.environ, "FRAMEWORK_SHELLS_SECRET": self.secret.decode()})
        try:
            async with asyncio.timeout(5):
                self.assertEqual(await process.wait(), 0)
        finally:
            if process.returncode is None:
                process.kill()
                _ = await process.wait()
        resumed = await self.client()
        # Explicit takeover avoids timing assumptions about disconnect cleanup.
        lease = result(await resumed.request({"op": "acquire", "takeover": True}))["generation"]
        _ = result(await resumed.request({"op": "write", "generation": lease, "data": "eAo="}))
        after = cast(Message, json.loads(await resumed.output()))
        self.assertEqual(after, {"nonce": before["nonce"], "count": 2})
