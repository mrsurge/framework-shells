from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from framework_shells.manager import FrameworkShellManager
from framework_shells.record import ShellRecord
from framework_shells.store import RuntimeStore


class FerrousRecordCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_python_fws_stamps_generic_child_marker_on_managed_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                os.environ,
                {
                    "FRAMEWORK_SHELLS_SECRET": "fws-child-marker-secret",
                    "FRAMEWORK_SHELLS_REPO_FINGERPRINT": "fwschildmarker",
                    "FRAMEWORK_SHELLS_FWS_SOCKETIO_URL": "http://127.0.0.1:18089",
                    "TE_FRAMEWORK_URL": "http://127.0.0.1:18089",
                    "FRAMEWORK_SHELLS_FWS_SOCKETIO_SERVER_PID": str(os.getpid()),
                    "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1",
                },
            ):
                store = RuntimeStore(base_dir=Path(tmp))
                manager = FrameworkShellManager(
                    store=store,
                    enable_dtach_proxy=False,
                    enable_procfs_process_discovery=False,
                )
                await asyncio.sleep(0)
                now = time.time()
                record = ShellRecord(
                    id="fs_child_marker",
                    command=["sh", "-c", "true"],
                    label="child-marker",
                    cwd=str(Path.cwd()),
                    env_overrides={},
                    pid=None,
                    status="created",
                    created_at=now,
                    updated_at=now,
                    autostart=True,
                    stdout_log=str(store.logs_dir / "fs_child_marker.stdout.log"),
                    stderr_log=str(store.logs_dir / "fs_child_marker.stderr.log"),
                    backend="proc",
                )

                env = manager._prepare_env(record)
                self.assertEqual(env.get("FRAMEWORK_SHELLS_FWS_CHILD"), "1")

                record.env_overrides["FRAMEWORK_SHELLS_FWS_CHILD"] = "0"
                env = manager._prepare_env(record)
                self.assertEqual(env.get("FRAMEWORK_SHELLS_FWS_CHILD"), "0")

    async def test_unsigned_ferrous_record_loads_for_log_inspection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                os.environ,
                {
                    "FRAMEWORK_SHELLS_SECRET": "ferrous-record-test-secret",
                    "FRAMEWORK_SHELLS_REPO_FINGERPRINT": "ferrousrecordtest",
                    "FRAMEWORK_SHELLS_FWS_SOCKETIO_URL": "",
                    "TE_FRAMEWORK_URL": "",
                    "FRAMEWORK_SHELLS_FWS_SOCKETIO_SERVER_PID": str(os.getpid()),
                },
            ):
                store = RuntimeStore(base_dir=Path(tmp))
                manager = FrameworkShellManager(
                    store=store,
                    enable_dtach_proxy=False,
                    enable_procfs_process_discovery=False,
                )
                shell_id = "frs_test_1"
                stdout_log = store.logs_dir / f"{shell_id}.stdout.log"
                stderr_log = store.logs_dir / f"{shell_id}.stderr.log"
                stdout_log.write_text('{"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n', encoding="utf-8")
                stderr_log.write_text("", encoding="utf-8")

                meta_path = store.metadata_dir / shell_id / "meta.json"
                meta_path.parent.mkdir(parents=True, exist_ok=True)
                now = time.time()
                record: dict[str, object] = {
                    "id": shell_id,
                    "spec_id": "app:test:worker",
                    "backend": "pipe",
                    "command": ["sh", "-c", "cat"],
                    "cwd": str(Path.cwd()),
                    "pid": os.getpid(),
                    "status": "running",
                    "exit_code": None,
                    "label": "app-worker:test",
                    "subgroups": ["test", "app-worker"],
                    "record_path": str(meta_path),
                    "stdout_log": str(stdout_log),
                    "stderr_log": str(stderr_log),
                    "io_metadata_log": None,
                    "pty_mode": None,
                    "autostart": True,
                    "ui": {},
                    "debug": {},
                    "created_at_ms": int(now * 1000),
                    "updated_at_ms": int(now * 1000),
                    "created_at": now,
                    "updated_at": now,
                    "run_id": "test-run",
                    "launcher_pid": os.getpid(),
                    "env_keys": [],
                    "env_overrides": {},
                    "uses_pty": False,
                    "uses_pipes": True,
                    "uses_dtach": False,
                    "runtime_id": store.runtime_id,
                    "signature": None,
                    "app_id": "test",
                    "parent_shell_id": None,
                    "is_app_worker": True,
                    "capabilities": {
                        "stdin_write": True,
                        "stdout_log": True,
                        "stderr_log": True,
                    },
                }
                meta_path.write_text(json.dumps(record), encoding="utf-8")

                loaded = await manager.load_shell_record(shell_id)
                self.assertIsNotNone(loaded)
                loaded_record = cast(ShellRecord, loaded)
                self.assertEqual(loaded_record.id, shell_id)
                self.assertEqual(loaded_record.backend, "pipe")
                self.assertTrue(loaded_record.adopted)

                listed = await manager.list_shells()
                self.assertIn(shell_id, {item.id for item in listed})

                inspected = await manager.inspect_logs(shell_id, stream="stdout", lines=20)
                self.assertEqual(inspected["shell_id"], shell_id)
                self.assertEqual(inspected["status"], "running")


if __name__ == "__main__":
    unittest.main()
