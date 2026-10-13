"""Signed discovery parity and disposable launcher-death acceptance."""
from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
import hashlib
import hmac
import importlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
from typing import Literal, cast
import unittest

from framework_shells.supervisor_attachment import AttachmentDescriptor, attach_supervisor
from framework_shells.supervisor_client import SupervisorClient, SupervisorCursor


def signed(fields: dict[str, object], secret: bytes = b"test-secret") -> bytes:
    body = json.dumps(fields, separators=(",", ":"), ensure_ascii=False)
    proof = base64.b64encode(hmac.digest(secret, b"fws-supervisor-attachment-v1\0" + body.encode(), "sha256")).decode()
    return json.dumps({"body": body, "proof": proof}).encode()


class AttachmentRecordTests(unittest.TestCase):
    def test_shared_descriptor_cases(self) -> None:
        fields: dict[str, object] = {"version": 1, "runtime_id": hashlib.sha256(b"test-secret").hexdigest()[:16],
            "shell": {"id": "fs_test", "label": None, "spec_id": None, "subgroups": []},
            "instance": "instance-test", "pid": 42, "supervisor_pid": 40,
            "socket": "/example/s", "stdout_log": "/example/out", "stderr_log": "/example/err",
            "command": ["cat"], "cwd": "/example"}
        cases = cast(list[dict[str, object]], json.loads((Path(__file__).parent / "fixtures/supervisor_attachment_cases.json").read_text()))
        for case in cases:
            with self.subTest(field=case["field"], value=case["value"]):
                record = {**fields, str(case["field"]): case["value"]}
                encoded = signed(record)
                if case["valid"]:
                    self.assertEqual(AttachmentDescriptor.decode(encoded, b"test-secret").pid, 42)
                else:
                    with self.assertRaises(ValueError):
                        _ = AttachmentDescriptor.decode(encoded, b"test-secret")
                with self.assertRaises(ValueError):
                    _ = AttachmentDescriptor.decode(encoded, b"wrong-secret")

    def test_private_bounded_nofollow_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "s.attachment.json"
            fields: dict[str, object] = {"version": 1, "runtime_id": hashlib.sha256(b"test-secret").hexdigest()[:16],
                "shell": {"id": "fs_test", "label": None, "spec_id": None, "subgroups": []},
                "instance": "instance-test", "pid": 42, "supervisor_pid": 40,
                "socket": str(root / "s"), "stdout_log": str(root / "out"), "stderr_log": str(root / "err"),
                "command": ["cat"], "cwd": str(root)}
            _ = path.write_bytes(signed(fields))
            path.chmod(0o600)
            self.assertEqual(AttachmentDescriptor.load(path, b"test-secret").pid, 42)
            link = root / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                _ = AttachmentDescriptor.load(link, b"test-secret")
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                _ = AttachmentDescriptor.load(path, b"test-secret")
            path.chmod(0o600)
            _ = path.write_bytes(b"x" * 65537)
            with self.assertRaises(ValueError):
                _ = AttachmentDescriptor.load(path, b"test-secret")
            _ = path.write_bytes(signed(fields))
            root.chmod(0o755)
            with self.assertRaises(ValueError):
                _ = AttachmentDescriptor.load(path, b"test-secret")
            root.chmod(0o700)


LAUNCHER = r'''
import asyncio,json,os,sys
from pathlib import Path
from framework_shells.supervisor_attachment import attach_supervisor
async def main():
    root=Path(sys.argv[1])
    child="import sys,json,uuid; nonce=uuid.uuid4().hex; count=0\nfor line in sys.stdin.buffer:\n count+=1; print(json.dumps({'nonce':nonce,'count':count}),flush=True)\n"
    config=root/'config'
    config.write_text(json.dumps({'socket':str(root/'s'),'stdout_log':str(root/'out'),'stderr_log':str(root/'err'),
        'command':[sys.executable,'-u','-c',child], 'cwd':str(root),
        'attachment':{'id':'fs_survived','label':'app-worker:example','spec_id':'example:worker','subgroups':['example','worker']}}))
    process=await asyncio.create_subprocess_exec(os.environ['FWS_SUPERVISOR_BINARY'],str(config),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
    ready=json.loads(await process.stdout.readline())
    attached=await attach_supervisor(root/'s.attachment.json',b'launcher-secret','fs_survived',backend='python')
    await attached.client.acquire()
    await attached.client.write(b'first\n')
    data=bytearray()
    while b'\n' not in data:
        data.extend((await attached.client.read()).data)
    print(json.dumps({'pid':ready['pid'],'supervisor_pid':process.pid,'first':json.loads(data),'bytes':len(data)}),flush=True)
    await asyncio.Event().wait()
asyncio.run(main())
'''


@unittest.skipUnless(os.environ.get("FWS_SUPERVISOR_BINARY"), "supervisor binary required")
class LauncherDeathTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_launcher_death_preserves_nonce_counter_and_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="fws-launcher-") as directory:
            root = Path(directory)
            env = {**os.environ, "FRAMEWORK_SHELLS_SECRET": "launcher-secret"}
            launcher = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", LAUNCHER, str(root),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
            clients: list[SupervisorClient] = []
            descriptor: AttachmentDescriptor | None = None
            try:
                assert launcher.stdout is not None
                async with asyncio.timeout(15):
                    first_line = await launcher.stdout.readline()
                    if not first_line:
                        assert launcher.stderr is not None
                        self.fail((await launcher.stderr.read()).decode())
                    ready = cast(dict[str, object], json.loads(first_line))
                    first = cast(dict[str, object], ready["first"])
                    launcher.kill()
                    _ = await launcher.wait()
                    self.assertEqual(launcher.returncode, -signal.SIGKILL)
                    descriptor = AttachmentDescriptor.load(root / "s.attachment.json", b"launcher-secret")
                    self.assertEqual(descriptor.pid, ready["pid"])
                    self.assertEqual(descriptor.supervisor_pid, ready["supervisor_pid"])
                    self.assertNotEqual(descriptor.supervisor_pid, launcher.pid)
                    backends: list[Literal["python", "native"]] = ["python"]
                    if os.environ.get("FWS_SUPERVISOR_CLIENT_EXTENSION"):
                        # Same isolated module loader as the common-client suite.
                        _ = importlib.import_module("tests.test_supervisor_client")
                        backends.append("native")
                    cursor = SupervisorCursor(stdout=int(str(ready["bytes"])))
                    for count, backend in enumerate(backends, start=2):
                        attached = await attach_supervisor(root / "s.attachment.json", b"launcher-secret", "fs_survived",
                                                           cursor=cursor, backend=backend)
                        clients.append(attached.client)
                        self.assertFalse(attached.status.exited)
                        self.assertEqual(attached.status.pid, ready["pid"])
                        self.assertEqual(attached.descriptor.shell.subgroups, ("example", "worker"))
                        _ = await attached.client.acquire(takeover=True)
                        _ = await attached.client.write(b"next\n")
                        data = bytearray()
                        while b"\n" not in data:
                            event = await attached.client.read()
                            self.assertEqual(event.stream, "stdout")
                            self.assertEqual(event.offset, cursor.stdout + len(data))
                            data.extend(event.data)
                        response = cast(dict[str, object], json.loads(data))
                        self.assertEqual(response, {"nonce": first["nonce"], "count": count})
                        cursor = SupervisorCursor(stdout=cursor.stdout + len(data))
                        await attached.client.close()
                    self.assertEqual((root / "out").stat().st_size, cursor.stdout)
            finally:
                if launcher.returncode is None:
                    launcher.kill()
                    _ = await launcher.wait()
                for client in clients:
                    await client.close()
                if descriptor is None and (root / "s.attachment.json").exists():
                    descriptor = AttachmentDescriptor.load(root / "s.attachment.json", b"launcher-secret")
                if descriptor is not None:
                    try:
                        attached = await attach_supervisor(root / "s.attachment.json", b"launcher-secret", "fs_survived", backend="python")
                        try:
                            await attached.client.stop()
                        finally:
                            await attached.client.close()
                    except Exception:
                        # Only these disposable process identities belong to this test.
                        with suppress(ProcessLookupError):
                            os.killpg(descriptor.pid, signal.SIGKILL)
                        with suppress(ProcessLookupError):
                            os.kill(descriptor.supervisor_pid, signal.SIGKILL)
