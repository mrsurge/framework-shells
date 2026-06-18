from __future__ import annotations

import unittest
from collections.abc import Mapping, Sequence
from typing import cast

from framework_shells.ferrous_framework import build_shutdown_response


class FerrousShutdownResponseTests(unittest.TestCase):
    def test_build_shutdown_response_includes_metrics_and_events(self) -> None:
        response = build_shutdown_response(
            kind="shutdown_group",
            target="demo",
            started_at_ms=1000,
            ended_at_ms=1250,
            root_pids=[101, 202],
            stats={
                "total": 2,
                "terminated": 2,
                "clean_exits": 1,
                "force_killed": 1,
                "errors": ["forced"],
            },
            events=["terminate shell fs_1", "sigkill shell fs_2"],
        )

        self.assertIs(response["ok"], True)
        self.assertEqual(response["kind"], "shutdown_group")
        self.assertEqual(response["target"], "demo")
        self.assertEqual(response["elapsed_ms"], 250)
        self.assertEqual(response["root_pids"], [101, 202])
        stats = cast(Mapping[str, object], response["stats"])
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["terminated"], 2)
        self.assertEqual(stats["clean_exits"], 1)
        self.assertEqual(stats["force_killed"], 1)
        self.assertEqual(stats["errors"], ["forced"])
        self.assertEqual(cast(Sequence[str], response["events"]), ["terminate shell fs_1", "sigkill shell fs_2"])


if __name__ == "__main__":
    unittest.main()
