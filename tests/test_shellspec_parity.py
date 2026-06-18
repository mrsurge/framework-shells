from __future__ import annotations

import json
import unittest
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from framework_shells.shellspec import ReadinessProbe, ShellSpec, parse_shellspec_data, render_shellspec

JSONValue = object
JSONMap = Mapping[str, JSONValue]

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "shellspec_parity_cases.json"


def _as_mapping(value: object, *, label: str) -> JSONMap:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a mapping")
    return cast(JSONMap, value)


def _as_sequence(value: object, *, label: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be a list")
    return cast(Sequence[object], value)


def _string_map(value: object, *, label: str) -> dict[str, str]:
    mapping = _as_mapping(value, label=label)
    return {str(key): str(item) for key, item in mapping.items()}


def _load_fixture() -> JSONMap:
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return _as_mapping(cast(object, raw), label="fixture")


def _readiness_payload(value: ReadinessProbe | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "type": value.type,
        "timeout": value.timeout,
        "pattern": value.pattern,
        "host": value.host,
        "port": value.port,
        "url": value.url,
        "status_codes": value.status_codes,
    }


def _rendered_payload(spec: ShellSpec) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": spec.id,
        "backend": spec.backend,
        "cwd": spec.cwd,
        "command": spec.normalized_command(),
        "env": dict(spec.env),
        "subgroups": list(spec.subgroups),
        "pipe": dict(spec.pipe),
        "pty_mode": spec.pty_mode,
        "readiness": _readiness_payload(spec.readiness),
        "autostart": spec.autostart,
    }
    return {key: value for key, value in payload.items() if value not in ({}, None)}


def _assert_matches_expected(
    test_case: unittest.TestCase,
    actual: object,
    expected: object,
    *,
    free_port_marker: str,
    free_port_values: list[object],
    path: str = "$",
) -> None:
    if expected == free_port_marker:
        free_port_values.append(actual)
        return
    if isinstance(expected, dict):
        actual_mapping = _as_mapping(actual, label=path)
        for key, expected_value in cast(Mapping[str, object], expected).items():
            test_case.assertIn(key, actual_mapping, f"{path}.{key}")
            _assert_matches_expected(
                test_case,
                actual_mapping[key],
                expected_value,
                free_port_marker=free_port_marker,
                free_port_values=free_port_values,
                path=f"{path}.{key}",
            )
        return
    if isinstance(expected, list):
        actual_sequence = _as_sequence(actual, label=path)
        expected_sequence = cast(Sequence[object], expected)
        test_case.assertEqual(len(actual_sequence), len(expected_sequence), path)
        for index, expected_value in enumerate(expected_sequence):
            _assert_matches_expected(
                test_case,
                actual_sequence[index],
                expected_value,
                free_port_marker=free_port_marker,
                free_port_values=free_port_values,
                path=f"{path}[{index}]",
            )
        return
    test_case.assertEqual(actual, expected, path)


def _assert_free_port_values(test_case: unittest.TestCase, values: Sequence[object]) -> None:
    if not values:
        return
    ports = [int(str(value)) for value in values]
    first = ports[0]
    for port in ports:
        test_case.assertEqual(port, first)
        test_case.assertGreater(port, 0)
        test_case.assertLessEqual(port, 65535)


class ShellspecParityTests(unittest.TestCase):
    def test_rendering_parity_fixtures(self) -> None:
        fixture = _load_fixture()
        marker = str(fixture["free_port_marker"])
        for raw_case in _as_sequence(fixture["cases"], label="cases"):
            case = _as_mapping(raw_case, label="case")
            with self.subTest(name=str(case["name"])):
                specs = parse_shellspec_data(case["document"])
                entry = str(case["entry"])
                spec = specs.get(entry) or next(iter(specs.values()))
                rendered = render_shellspec(
                    spec,
                    ctx=_as_mapping(case["ctx"], label="ctx"),
                    env=_string_map(case["env"], label="env"),
                )
                free_port_values: list[object] = []
                _assert_matches_expected(
                    self,
                    _rendered_payload(rendered),
                    case["expect"],
                    free_port_marker=marker,
                    free_port_values=free_port_values,
                )
                _assert_free_port_values(self, free_port_values)


if __name__ == "__main__":
    unittest.main()
