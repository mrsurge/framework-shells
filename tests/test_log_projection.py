from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import TypedDict, cast

from framework_shells.log_projection import Codec, RawReference, WindowAction, project_record, window_start


class RecordCase(TypedDict):
    source: str
    codec: Codec
    budget: int
    expected: dict[str, object]


class WindowCase(TypedDict):
    total: int
    current: int
    count: int
    shift: int
    action: WindowAction
    expected: int


class Fixtures(TypedDict):
    version: int
    records: list[RecordCase]
    windows: list[WindowCase]


class LogProjectionTests(unittest.TestCase):
    def test_shared_fixtures(self) -> None:
        path = Path(__file__).parent / "fixtures/log_projection_cases.json"
        fixtures = cast(Fixtures, json.loads(path.read_text()))
        self.assertEqual(fixtures["version"], 1)
        for case in fixtures["records"]:
            data = case["source"].encode()
            raw = RawReference("fixture", 17, 17 + len(data))
            result = project_record(data, raw, codec=case["codec"], max_bytes=case["budget"]).to_dict()
            expected = dict(case["expected"])
            expected["raw"] = {"generation": "fixture", "byte_start": 17, "byte_end": 17 + len(data)}
            self.assertEqual(result, expected)
            self.assertLessEqual(len(str(result["text"]).encode()), case["budget"])
        for case in fixtures["windows"]:
            self.assertEqual(window_start(case["total"], case["current"], case["count"], case["shift"], case["action"]), case["expected"])

    def test_reference_and_budget_validation(self) -> None:
        with self.assertRaises(ValueError):
            _ = project_record(b"x", RawReference("x", 0, 2))
        with self.assertRaises(ValueError):
            _ = window_start(10, 0, 0, 1, "tail")

    def test_parser_ceiling_preserves_raw_reference(self) -> None:
        data = b'{"huge":"' + b"x" * 1000 + b'"}'
        raw = RawReference("x", 0, len(data))
        result = project_record(data, raw, codec="json", max_bytes=32, parse_bytes=64)
        self.assertEqual(result.diagnostic, "parse_budget_exceeded")
        self.assertEqual(result.raw, raw)


if __name__ == "__main__":
    unittest.main()
