from __future__ import annotations
import json
import unittest
from dataclasses import asdict
from pathlib import Path
from typing import TypedDict, NotRequired, cast
from framework_shells.msgpack_observation import decode_frame


class Case(TypedDict):
    hex: str
    expected: NotRequired[dict[str, object]]
    error: NotRequired[bool]
    incomplete: NotRequired[bool]


class Fixtures(TypedDict):
    version: int
    cases: list[Case]


class MessagePackTests(unittest.TestCase):
    def test_shared_fixtures_and_all_split_points(self) -> None:
        fixtures = cast(Fixtures, json.loads((Path(__file__).parent / "fixtures/msgpack_observation_cases.json").read_text()))
        self.assertEqual(fixtures["version"], 1)
        for case in fixtures["cases"]:
            data = bytes.fromhex(case["hex"])
            with self.subTest(hex=case["hex"]):
                if case.get("error"):
                    with self.assertRaises(ValueError):
                        _ = decode_frame(data)
                    continue
                frame = decode_frame(data)
                if case.get("incomplete"):
                    self.assertIsNone(frame)
                    continue
                self.assertIsNotNone(frame)
                assert frame is not None
                self.assertEqual(asdict(frame), case.get("expected"))
                for split in range(frame.consumed):
                    self.assertIsNone(decode_frame(data[:split]))
                    self.assertEqual(decode_frame(data[:split] + data[split:]), frame)

    def test_frame_budget(self) -> None:
        self.assertIsNone(decode_frame(b"\xdb\xff\xff\xff\xff"))
        with self.assertRaises(ValueError):
            _ = decode_frame(b"\xc4\x20" + b"x" * 8, max_bytes=10)
