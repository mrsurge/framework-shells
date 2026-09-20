from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from framework_shells.supervisor_transport import MAX_FRAME_BYTES, read_frame, write_frame


class SupervisorTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_split_coalesced_and_empty_frames(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"\0\0")
        reader.feed_data(b"\0\2\0\xff\0\0\0\0\0\0\0\1\n")
        reader.feed_eof()
        self.assertEqual(await read_frame(reader), b"\0\xff")
        self.assertEqual(await read_frame(reader), b"")
        self.assertEqual(await read_frame(reader), b"\n")
        self.assertIsNone(await read_frame(reader))

    async def test_invalid_lengths_and_truncation(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data((MAX_FRAME_BYTES + 1).to_bytes(4, "big"))
        async with asyncio.timeout(1):
            with self.assertRaises(ValueError):
                _ = await read_frame(reader)
        for partial in (b"\0", b"\0\0\0\3x"):
            reader = asyncio.StreamReader()
            reader.feed_data(partial)
            reader.feed_eof()
            with self.assertRaises(asyncio.IncompleteReadError):
                _ = await read_frame(reader)

    @unittest.skipUnless(os.environ.get("FWS_SUPERVISOR_FRAMING_PEER"), "build/set disposable Rust framing peer")
    async def test_rust_python_uds_interoperability(self) -> None:
        executable = os.environ["FWS_SUPERVISOR_FRAMING_PEER"]
        done: asyncio.Future[list[bytes]] = asyncio.get_running_loop().create_future()

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            frames: list[bytes] = []
            try:
                async with asyncio.timeout(5):
                    while (frame := await read_frame(reader)) is not None:
                        frames.append(frame)
                        await write_frame(writer, frame)
                done.set_result(frames)
            except Exception as exc:
                done.set_exception(exc)
            finally:
                writer.close()
                await writer.wait_closed()

        with tempfile.TemporaryDirectory(prefix="fws-") as directory:
            path = Path(directory) / "peer.sock"
            server = await asyncio.start_unix_server(serve, path)
            async with server:
                child = await asyncio.create_subprocess_exec(executable, str(path))
                try:
                    async with asyncio.timeout(10):
                        self.assertEqual(await child.wait(), 0)
                        self.assertEqual(await done, [b"", b"\0\xff\n\x80", b"\xa5" * MAX_FRAME_BYTES])
                finally:
                    if child.returncode is None:
                        child.kill()
                        _ = await child.wait()


if __name__ == "__main__":
    unittest.main()
