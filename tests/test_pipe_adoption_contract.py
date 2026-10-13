"""Characterize ordinary pipe ownership before adding resumable transports."""
from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from framework_shells.manager import FrameworkShellManager
from framework_shells.store import RuntimeStore


class PipeAdoptionContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_manager_observes_but_cannot_use_live_pipe(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "FRAMEWORK_SHELLS_SECRET": "disposable-adoption-contract",
            "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1",
        }):
            store = RuntimeStore(base_dir=Path(directory))
            owner = FrameworkShellManager(store=store, enable_procfs_process_discovery=False)
            record = await owner.spawn_shell_pipe(
                ["sh", "-c", 'while IFS= read -r line; do printf "ack:%s\\n" "$line"; done'],
                label="adoption-contract", spec_id="adoption-contract", subgroups=["isolated-test"],
            )
            try:
                output = await owner.subscribe_output_bytes(record.id)
                fresh = FrameworkShellManager(store=store, enable_procfs_process_discovery=False)
                loaded = await fresh.load_shell_record(record.id)
                assert loaded is not None
                self.assertEqual((loaded.id, loaded.pid, loaded.label, loaded.subgroups),
                                 (record.id, record.pid, record.label, record.subgroups))
                self.assertEqual(loaded.status, "running")
                self.assertIsNone(fresh.get_pipe_state(record.id))
                capabilities = await fresh.get_shell_capabilities(loaded)
                for key in ("stdin_write", "stdin_eof", "stdout_subscribe", "reattach"):
                    self.assertFalse(capabilities[key], key)
                with self.assertRaises(KeyError):
                    await fresh.write_to_pipe(record.id, "not-delivered\n")
                with self.assertRaises(KeyError):
                    await fresh.send_pipe_eof(record.id)
                await owner.write_to_pipe(record.id, "still-owned\n")
                received = bytearray()
                async with asyncio.timeout(5):
                    while b"\n" not in received:
                        received.extend(await output.get())
                self.assertEqual(bytes(received), b"ack:still-owned\n")
                # Metadata-based Stop remains possible without adopting descriptors.
                await fresh.terminate_shell(record.id, force=True)
                state = owner.get_pipe_state(record.id)
                if state is not None:
                    async with asyncio.timeout(5):
                        _ = await state.process.wait()
            finally:
                await owner.terminate_shell(record.id, force=True)


if __name__ == "__main__":
    unittest.main()
