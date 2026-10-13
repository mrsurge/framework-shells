from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if importlib.util.find_spec("msgspec") is None:
    raise unittest.SkipTest("isolated benchmark msgspec binding required")

from scripts.uds_bench.rpc import LANES, Router, Workload, trial


class FakeTransport:
    stdout_log: Path = Path("unused")
    stderr_log: Path = Path("unused")

    async def send(self, data: bytes) -> None:
        raise NotImplementedError

    async def read(self) -> bytes:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class RpcRouterTests(unittest.IsolatedAsyncioTestCase):
    def router(self) -> Router:
        router = Router(FakeTransport(), Workload("test", 1, 0, 1, 0))
        router.pending[1] = asyncio.get_running_loop().create_future()
        return router

    async def test_notification_then_response(self) -> None:
        router = self.router()
        router.dispatch({"jsonrpc": "2.0", "method": "bench.push", "params": {
            "request_id": 1, "ordinal": 1, "index": 0, "payload": ""}})
        router.dispatch({"jsonrpc": "2.0", "id": 1, "result": {"ok": True, "ordinal": 1, "payload": ""}})
        self.assertEqual(router.notifications, 1)
        self.assertFalse(router.pending)

    async def test_missing_and_duplicate_notifications_fail(self) -> None:
        router = self.router()
        with self.assertRaisesRegex(ValueError, "notification count"):
            router.dispatch({"jsonrpc": "2.0", "id": 1, "result": {"ok": True, "ordinal": 1, "payload": ""}})
        router = self.router()
        notification: dict[str, object] = {"jsonrpc": "2.0", "method": "bench.push", "params": {
            "request_id": 1, "ordinal": 1, "index": 0, "payload": ""}}
        router.dispatch(notification)
        with self.assertRaisesRegex(ValueError, "notification"):
            router.dispatch(notification)

    async def test_unknown_ids_and_wrong_payloads_fail(self) -> None:
        router = self.router()
        with self.assertRaisesRegex(ValueError, "unknown"):
            router.dispatch({"jsonrpc": "2.0", "id": 99, "result": {}})
        with self.assertRaisesRegex(ValueError, "wrong result"):
            router.dispatch({"jsonrpc": "2.0", "id": 1, "result": {"ok": True, "ordinal": 1, "payload": "corrupt"}})


@unittest.skipUnless(all(os.environ.get(key) for key in
    ("FWS_BENCH_RPC_CHILD", "FWS_SUPERVISOR_BINARY", "FWS_BENCH_NATIVE_EXTENSION", "FWS_SUPERVISOR_CLIENT_EXTENSION")), "native benchmark artifacts required")
class RpcProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_lanes_with_fragmented_notifications(self) -> None:
        for lane in LANES:
            with self.subTest(lane=lane), patch.dict(os.environ):
                result = await trial(lane, Workload("fragmented", 4, 16384, 2, 4096), 12,
                    Path(os.environ["FWS_BENCH_RPC_CHILD"]), Path(os.environ["FWS_SUPERVISOR_BINARY"]),
                    Path(os.environ["FWS_BENCH_NATIVE_EXTENSION"]))
                self.assertEqual(result["completed"], 12)
                self.assertEqual(result["notifications"], 24)
                self.assertTrue(result["raw_log_verified"])
                self.assertTrue(result["valid"])

    async def test_native_socket_handoff_preserves_prebuffer_and_eof(self) -> None:
        from scripts.uds_bench.rpc import NativeSocketReader, load_native
        factory = load_native(Path(os.environ["FWS_BENCH_NATIVE_EXTENSION"]))
        sent = asyncio.Event()
        release = asyncio.Event()
        ended = asyncio.Event()
        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                writer.write(b"A" + bytes(range(256)))
                await writer.drain()
                sent.set()
                await release.wait()
            finally:
                writer.close()
                await writer.wait_closed()
                ended.set()
        with tempfile.TemporaryDirectory(prefix="handoff-") as directory:
            path = str(Path(directory) / "s")
            listener = await asyncio.start_unix_server(serve, path)
            async with listener:
                reader, writer = await asyncio.open_unix_connection(path)
                native: NativeSocketReader | None = None
                try:
                    async with asyncio.timeout(5):
                        await sent.wait()
                        self.assertEqual(await reader.readexactly(1), b"A")
                        native = NativeSocketReader(reader, writer, factory)
                        self.assertEqual(await native.readexactly(256), bytes(range(256)))
                        release.set()
                        with self.assertRaises(asyncio.IncompleteReadError):
                            await native.readexactly(1)
                finally:
                    release.set()
                    if native is not None:
                        native.close()
                    writer.close()
                    await writer.wait_closed()
                    await ended.wait()
