from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import TypedDict, cast

from framework_shells.persistent_shell import (
    AttachmentState, ShutdownIntent, WriterLease, parse_survival, select_shutdown,
)


class SurvivalCase(TypedDict):
    value: object
    expected: bool | None


class ShutdownCase(TypedDict):
    name: str
    parents: dict[str, int | None]
    roots: list[int]
    survival_roots: list[int]
    intent: str
    terminate: list[int]
    preserved: list[int]


class Cases(TypedDict):
    survival: list[SurvivalCase]
    shutdown: list[ShutdownCase]


class PersistentShellTests(unittest.TestCase):
    def test_shared_policy_fixtures(self) -> None:
        cases = cast(Cases, json.loads((Path(__file__).parent / "fixtures/persistent_shell_cases.json").read_text()))
        self.assertFalse(parse_survival())
        for case in cases["survival"]:
            with self.subTest(value=case["value"]):
                if case["expected"] is None:
                    with self.assertRaises(ValueError):
                        parse_survival(case["value"])
                else:
                    self.assertEqual(parse_survival(case["value"]), case["expected"])
        for case in cases["shutdown"]:
            with self.subTest(name=case["name"]):
                selection = select_shutdown(
                    {int(pid): parent for pid, parent in case["parents"].items()},
                    set(case["roots"]), set(case["survival_roots"]), ShutdownIntent(case["intent"]),
                )
                self.assertEqual(sorted(selection.terminate), case["terminate"])
                self.assertEqual(sorted(selection.preserved), case["preserved"])

    def test_writer_fencing_and_disconnect_not_eof(self) -> None:
        state = AttachmentState("instance-a")
        old = state.acquire("manager-a")
        with self.assertRaises(ValueError):
            state.acquire("manager-b")
        current = state.acquire("manager-b", takeover=True)
        self.assertFalse(state.can_write(old))
        state.detach(old)
        self.assertTrue(state.can_write(current))
        self.assertFalse(state.can_write(WriterLease("wrong-instance", current.generation, current.connection)))
        with self.assertRaises(ValueError):
            state.eof(old)
        state.detach(current)
        resumed = state.acquire("manager-c")
        self.assertGreater(resumed.generation, current.generation)
        self.assertTrue(state.can_write(resumed))
        state.eof(resumed)
        self.assertFalse(state.can_write(resumed))
        with self.assertRaises(ValueError):
            state.acquire("manager-d", takeover=True)

    def test_exit_revokes_writer(self) -> None:
        state = AttachmentState("instance-a")
        writer = state.acquire("manager-a")
        state.child_exited()
        self.assertFalse(state.can_write(writer))
        with self.assertRaises(ValueError):
            state.acquire("manager-b", takeover=True)


if __name__ == "__main__":
    unittest.main()
