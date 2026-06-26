from __future__ import annotations

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
