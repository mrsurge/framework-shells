from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import unittest

from scripts.uds_bench.codecs import CODECS, ROOT, Codec, Envelope
from scripts.uds_bench.run import trial


BINDINGS_AVAILABLE = (importlib.util.find_spec("capnp") is not None
    and importlib.util.find_spec("msgspec") is not None
    and importlib.util.find_spec("flatbuffers") is not None
    and (ROOT / ".external/uds-generated/FwsBench/Envelope.py").exists())


@unittest.skipUnless(BINDINGS_AVAILABLE, "isolated benchmark bindings/generated files required")
class CandidateCodecTests(unittest.IsolatedAsyncioTestCase):
    def test_bytes_and_metadata(self) -> None:
        for name in CODECS:
            codec = Codec(name)
            for data in (b"", bytes(range(256)), b"\x00\xff" * 16384):
                value = Envelope(data)
                self.assertEqual(codec.decode(codec.encode(value)), value, name)
                if name != "raw":
                    for number in (0, 2**64 - 1):
                        value = Envelope(data, version=65535, generation=number, stream=255, offset=number)
                        self.assertEqual(codec.decode(codec.encode(value)), value, name)

    def test_truncated_payload(self) -> None:
        for name in ("json", "json-msgspec", "flatbuffers", "capnp"):
            codec = Codec(name)
            data = codec.encode(Envelope(bytes(range(256))))
            with self.assertRaises(Exception, msg=name):
                codec.decode(data[:len(data) // 2])

    def test_msgspec_matches_json_wire(self) -> None:
        value = Envelope(bytes(range(256)))
        self.assertEqual(Codec("json-msgspec").encode(value), Codec("json").encode(value))

    async def test_python_processes(self) -> None:
        for name in CODECS:
            result = await trial(name, 4096, 2, 8)
            self.assertTrue(result["valid"], name)

    @unittest.skipUnless(os.environ.get("FWS_BENCH_RUST_BINARY"), "Rust bench binary required")
    async def test_rust_and_mixed_processes(self) -> None:
        binary = Path(os.environ["FWS_BENCH_RUST_BINARY"])
        for name in CODECS:
            for client, peer in (("python", "rust"), ("rust", "python"), ("rust", "rust")):
                for size in (0, 32768):
                    result = await trial(name, size, 2, 8, client, peer, binary)
                    self.assertTrue(result["valid"], (name, client, peer, size))
