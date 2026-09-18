from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from framework_shells.api.fastapi_router import get_log_raw, get_log_window
from framework_shells.log_codecs import log_codecs
from framework_shells.log_inspection import inspect_log_file
from framework_shells.log_window import LineIndex, mark_log_reset
from framework_shells.manager import FrameworkShellManager
from framework_shells.events import EventType, get_event_bus
from framework_shells.shellspec import ShellSpec, render_shellspec, parse_shellspec_data
from framework_shells.store import RuntimeStore


class ProjectionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_output_event_is_already_visible_to_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "FRAMEWORK_SHELLS_SECRET": "projection-event-test-secret",
            "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1",
        }):
            manager = FrameworkShellManager(store=RuntimeStore(base_dir=Path(directory)),
                enable_procfs_process_discovery=False)
            bus = get_event_bus()
            queue = bus.subscribe()
            record = await manager.spawn_shell([sys.executable, "-c",
                "import os,time; os.write(1,b'visible before exit\\n'); time.sleep(30)"])
            try:
                async with asyncio.timeout(5):
                    while True:
                        event = await queue.get()
                        if event.shell_id == record.id and event.type == EventType.LOG_CHUNK:
                            break
                window = await manager.get_log_window(record.id)
                self.assertEqual(window.records[0].text, "visible before exit\n")
            finally:
                await manager.terminate_shell(record.id, force=True)
                bus.unsubscribe(queue)

    async def test_codec_record_window_raw_and_reload(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "FRAMEWORK_SHELLS_SECRET": "projection-test-secret",
            "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1",
        }):
            store = RuntimeStore(base_dir=Path(directory))
            manager = FrameworkShellManager(store=store, enable_procfs_process_discovery=False)
            record = await manager.spawn_shell_pipe(["unused"], autostart=False,
                log_codecs={"stdout": "messagepack", "stderr": "text"})
            frame = bytes.fromhex("81a2696407")
            _ = Path(record.stdout_log).write_bytes(frame + frame)
            _ = Path(record.stderr_log).write_bytes(b"diagnostic\n")
            loaded = await manager.load_shell_record(record.id)
            self.assertIsNotNone(loaded)
            if loaded is None:
                return
            self.assertEqual(loaded.log_codecs, record.log_codecs)
            self.assertTrue(loaded.verify(store.secret))
            window = await manager.get_log_window(record.id, count=1)
            self.assertEqual((window.start, window.end, window.total), (1, 2, 2))
            self.assertEqual(window.records[0].text, '{"id":7}')
            self.assertEqual(await manager.get_log_raw(record.id, window.records[0].raw), frame)
            response = await get_log_window(record.id, manager, count=1)
            self.assertTrue(response["ok"])
            raw_response = await get_log_raw(record.id, manager, window.generation, 5, 10, limit=2)
            self.assertEqual(raw_response["data"], {"hex": "81a2", "next_offset": 2, "eof": False})
            await manager.emit_log_reset(record.id, "stdout")
            with self.assertRaisesRegex(ValueError, "stale"):
                _ = await manager.get_log_raw(record.id, window.records[0].raw)
            self.assertEqual(Path(record.stdout_log).read_bytes(), frame + frame)
            await asyncio.sleep(0)

    async def test_inspection_decodes_before_filtering(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            # {"jsonrpc":"2.0", "method":"ping"}, followed by an incomplete object.
            frame = bytes.fromhex("82a76a736f6e727063a3322e30a66d6574686f64a470696e67")
            _ = path.write_bytes(frame + b"\x81")
            result = await inspect_log_file(path, stream="stdout", lines=10,
                max_bytes=65536, codec="messagepack", query="ping", format_filter="jsonrpc")
            self.assertEqual(len(result["records"]), 1)
            record = result["records"][0]
            self.assertEqual(record.get("byte_end"), len(frame))
            self.assertIn("jsonrpc", record.get("formats_detected", []))
            self.assertEqual(path.read_bytes(), frame + b"\x81")

    async def test_cross_cache_reset_with_regrowth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            _ = path.write_bytes(b"old\n")
            index = LineIndex(path)
            try:
                reference = index.window().records[0].raw
                _ = path.write_bytes(b"replacement is longer\n")
                mark_log_reset(path)
                with self.assertRaisesRegex(ValueError, "stale"):
                    _ = index.raw(reference)
            finally:
                index.close()

    def test_codec_templates_and_validation(self) -> None:
        spec = ShellSpec(id="test", command=["unused"], log_codecs={"stdout": "${ctx:CODEC}"})
        self.assertEqual(render_shellspec(spec, ctx={"CODEC": "messagepack"}).log_codecs,
            {"stdout": "messagepack"})
        parsed = parse_shellspec_data(json.loads('{"version":"1","shells":{"test":{"command":["unused"],"log_codecs":{"stdout":"json"}}}}'))
        self.assertEqual(parsed["test"].log_codecs, {"stdout": "json"})
        invalid: list[object] = [{"stdin": "text"}, {"stdout": "msgpack"}, {"stdout": True}, []]
        for bad in invalid:
            with self.assertRaises(ValueError):
                _ = log_codecs(bad)
