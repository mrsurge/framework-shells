from __future__ import annotations

import asyncio
import unittest
from scripts.bench_uds_envelope import LIMIT, decode, encode, read, trial


class EnvelopeBenchmarkTests(unittest.IsolatedAsyncioTestCase):
    def test_binary_roundtrip(self) -> None:
        for codec in ("raw", "json"):
            for payload in (b"", bytes(range(256)), b"x" * 32768):
                self.assertEqual(decode(encode(payload, codec), codec), payload)

    async def test_oversized_header_rejected_without_body(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data((LIMIT + 1).to_bytes(4, "big"))
        with self.assertRaises(ValueError):
            _ = await read(reader)

    async def test_disposable_process_controls(self) -> None:
        for codec in ("raw", "json"):
            result = await trial(codec, 128, 2, 8)
            self.assertEqual(result["completed"], 8)
            self.assertEqual(result["errors"], 0)
