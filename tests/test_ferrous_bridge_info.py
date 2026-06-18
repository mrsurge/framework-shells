from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import cast

from framework_shells.ferrous_framework import FERROUS_BRIDGE_API, FERROUS_BRIDGE_SUPPORTS, ferrous_bridge_info


class FerrousBridgeInfoTests(unittest.TestCase):
    def test_bridge_info_reports_required_capabilities(self) -> None:
        info = ferrous_bridge_info()
        self.assertEqual(info["bridge_api"], FERROUS_BRIDGE_API)
        self.assertIsInstance(info["framework_shells_version"], str)
        supports = cast(Mapping[str, object], info["supports"])
        for name, expected in FERROUS_BRIDGE_SUPPORTS.items():
            self.assertIs(supports.get(name), expected)


if __name__ == "__main__":
    unittest.main()
