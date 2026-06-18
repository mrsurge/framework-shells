from __future__ import annotations

import os
import re
import shlex
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeAlias, cast

import yaml

from .native_pipe import (
    NATIVE_TERMINAL_PIPE_TESTING_MODE,
    NATIVE_TERMINAL_PLACEHOLDER_COMMAND,
    PYTHON_TERMINAL_PIPE_TESTING_MODE,
    TERMINAL_FALLBACK_COMMAND,
    normalize_pipe_config,
)
from .record import normalize_launch_backend

ScalarValue: TypeAlias = str | int | float | bool | None
SpecValue: TypeAlias = ScalarValue | list["SpecValue"] | dict[str, "SpecValue"]
SpecMap: TypeAlias = dict[str, SpecValue]


@dataclass(frozen=True)
class ReadinessProbe:
    type: str  # "stdout_regex" | "tcp_port" | "http_ok"
    timeout: float = 30.0
    # stdout_regex
    pattern: str | None = None
    # tcp_port
    host: str = "127.0.0.1"
    port: int | str | None = None
    # http_ok
    url: str | None = None
    status_codes: list[int] = field(default_factory=lambda: [200])


@dataclass(frozen=True)
class RestartPolicy:
    policy: str = "never"  # "never" | "on-failure" | "always"
    max_restarts: int = 3
    backoff_ms: int = 1000


@dataclass(frozen=True)
class ShellSpec:
    id: str
    command: str | list[str]
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    subgroups: list[str] = field(default_factory=list)
    ui: dict[str, object] = field(default_factory=dict)
    debug: dict[str, object] = field(default_factory=dict)
    pipe: dict[str, object] = field(default_factory=dict)
    pty_mode: str = "raw"  # "raw" | "interactive"
    readiness: ReadinessProbe | None = None
    restart: RestartPolicy = field(default_factory=RestartPolicy)
    backend: str = "proc"  # "proc" | "pty" | "pipe" (legacy "dtach" aliases to "pty")
    autostart: bool = True

    def normalized_command(self) -> list[str]:
        if isinstance(self.command, str):
            return shlex.split(self.command)
        return [str(part) for part in self.command]


_TEMPLATE_RE = re.compile(r"\$\{([^}]+)\}")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        sockaddr_obj = cast(object, s.getsockname())
        if isinstance(sockaddr_obj, tuple):
            sockaddr = cast(tuple[object, ...], sockaddr_obj)
            if len(sockaddr) >= 2 and isinstance(sockaddr[1], (str, int)):
                return int(sockaddr[1])
        raise RuntimeError("unexpected socket address shape")


def _as_spec_map(value: object) -> SpecMap:
    return cast(SpecMap, value) if isinstance(value, dict) else {}


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _string_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    value_map = cast(dict[object, object], value)
    return {str(k): str(v) for k, v in value_map.items()}


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    value_list = cast(list[object], value)
    return [str(item) for item in value_list]


def _float_or_default(value: object, default: float) -> float:
    if not isinstance(value, (str, int, float)):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _int_or_default(value: object, default: int) -> int:
    if not isinstance(value, (str, int, float)):
        return default
    try:
        return int(value)
    except Exception:
        return default


def _int_list(value: object, default: list[int] | None = None) -> list[int]:
    if not isinstance(value, list):
        return list(default or [])
    out: list[int] = []
    for item in cast(list[object], value):
        if not isinstance(item, (str, int, float)):
            continue
        try:
            out.append(int(item))
        except Exception:
            continue
    return out or list(default or [])


def _render_string(template: str, *, ctx: Mapping[str, object], env: Mapping[str, str], state: SpecMap) -> str:
    def _replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if not key:
            return ""

        if key == "free_port":
            if "free_port" not in state:
                state["free_port"] = _find_free_port()
            return str(state["free_port"])

        if key.startswith("env:"):
            name = key.split(":", 1)[1]
            return str(env.get(name, ""))

        if key.startswith("ctx:"):
            name = key.split(":", 1)[1]
            return str(ctx.get(name, ""))

        # Convenience: ${PROJECT_ROOT} resolves from ctx first, then env.
        if key in ctx:
            return str(ctx.get(key, ""))
        return str(env.get(key, ""))

    return _TEMPLATE_RE.sub(_replace, template)


def _render_value(value: SpecValue, *, ctx: Mapping[str, object], env: Mapping[str, str], state: SpecMap) -> SpecValue:
    if isinstance(value, str):
        return _render_string(value, ctx=ctx, env=env, state=state)
    if isinstance(value, list):
        return [_render_value(v, ctx=ctx, env=env, state=state) for v in value]
    if isinstance(value, dict):
        return {str(k): _render_value(v, ctx=ctx, env=env, state=state) for k, v in value.items()}
    return value


def render_shellspec(spec: ShellSpec, *, ctx: Mapping[str, object] | None = None, env: Mapping[str, str] | None = None) -> ShellSpec:
    """Render templates in a spec (e.g. ${free_port}, ${ctx:APP_ID}, ${env:HOME}).

    Rendering is per-shell: `${free_port}` is stable within a rendered spec, but
    different shells render with different values.
    """
    ctx_map: dict[str, object] = dict(ctx or {})
    env_map: dict[str, str] = dict(env or os.environ)
    state: SpecMap = {}

    rendered = cast(SpecMap, _render_value(
        cast(SpecValue, {
            "id": spec.id,
            "command": spec.command,
            "cwd": spec.cwd,
            "env": dict(spec.env or {}),
            "subgroups": list(spec.subgroups or []),
            "ui": dict(spec.ui or {}),
            "debug": dict(spec.debug or {}),
            "pipe": dict(spec.pipe or {}),
            "pty_mode": spec.pty_mode,
            "readiness": None,
            "restart": {
                "policy": spec.restart.policy,
                "max_restarts": spec.restart.max_restarts,
                "backoff_ms": spec.restart.backoff_ms,
            },
            "backend": spec.backend,
            "autostart": spec.autostart,
        }),
        ctx=ctx_map,
        env=env_map,
        state=state,
    ))

    readiness = spec.readiness
    if readiness:
        rendered_probe_raw = cast(SpecMap, _render_value(
            cast(SpecValue, {
                "type": readiness.type,
                "timeout": readiness.timeout,
                "pattern": readiness.pattern,
                "host": readiness.host,
                "port": readiness.port,
                "url": readiness.url,
                "status_codes": list(readiness.status_codes or [200]),
            }),
            ctx=ctx_map,
            env=env_map,
            state=state,
        ))

        port = rendered_probe_raw.get("port")
        try:
            if isinstance(port, str) and port.strip().isdigit():
                port = int(port.strip())
        except Exception:
            pass

        readiness = ReadinessProbe(
            type=str(rendered_probe_raw.get("type") or readiness.type),
            timeout=_float_or_default(rendered_probe_raw.get("timeout"), readiness.timeout),
            pattern=_string_or_none(rendered_probe_raw.get("pattern")),
            host=str(rendered_probe_raw.get("host") or readiness.host),
            port=port if isinstance(port, (int, str)) else None,
            url=_string_or_none(rendered_probe_raw.get("url")),
            status_codes=_int_list(rendered_probe_raw.get("status_codes"), [200]),
        )

    restart_raw = _as_spec_map(rendered.get("restart"))
    restart = RestartPolicy(
        policy=str(restart_raw.get("policy") or spec.restart.policy),
        max_restarts=_int_or_default(restart_raw.get("max_restarts"), spec.restart.max_restarts),
        backoff_ms=_int_or_default(restart_raw.get("backoff_ms"), spec.restart.backoff_ms),
    )

    return ShellSpec(
        id=str(rendered.get("id") or spec.id),
        command=(
            str(rendered["command"])
            if isinstance(rendered.get("command"), str)
            else _string_list(rendered.get("command"))
        ),
        cwd=_string_or_none(rendered.get("cwd")),
        env=_string_dict(rendered.get("env")),
        subgroups=_string_list(rendered.get("subgroups")),
        ui=cast(dict[str, object], _as_spec_map(rendered.get("ui"))),
        debug=cast(dict[str, object], _as_spec_map(rendered.get("debug"))),
        pipe=cast(dict[str, object], _as_spec_map(rendered.get("pipe"))),
        pty_mode=str(rendered.get("pty_mode") or spec.pty_mode or "raw"),
        readiness=readiness,
        restart=restart,
        backend=normalize_launch_backend(str(rendered.get("backend") or spec.backend)),
        autostart=bool(rendered.get("autostart") if "autostart" in rendered else spec.autostart),
    )


def _parse_readiness(raw: object) -> ReadinessProbe | None:
    if not isinstance(raw, dict):
        return None
    raw_map = _as_spec_map(cast(object, raw))
    type_val = raw_map.get("type")
    if not type_val:
        return None
    status_codes = raw_map.get("status_codes") or raw_map.get("statusCodes") or [200]
    if not isinstance(status_codes, list):
        status_codes = [200]
    port_val = raw_map.get("port")
    if isinstance(port_val, str):
        port_val = int(port_val.strip()) if port_val.strip().isdigit() else port_val
    elif isinstance(port_val, (int, float)):
        port_val = int(port_val)
    else:
        port_val = None
    return ReadinessProbe(
        type=str(type_val),
        timeout=_float_or_default(raw_map.get("timeout", 30.0), 30.0),
        pattern=_string_or_none(raw_map.get("pattern")),
        host=str(raw_map.get("host", "127.0.0.1")),
        port=port_val,
        url=_string_or_none(raw_map.get("url")),
        status_codes=_int_list(status_codes, [200]),
    )


def _parse_restart(raw: object) -> RestartPolicy:
    if not isinstance(raw, dict):
        return RestartPolicy()
    raw_map = _as_spec_map(cast(object, raw))
    return RestartPolicy(
        policy=str(raw_map.get("policy", "never")),
        max_restarts=_int_or_default(raw_map.get("max_restarts", 3), 3),
        backoff_ms=_int_or_default(raw_map.get("backoff_ms", 1000), 1000),
    )


def _spec_from_dict(shell_id: str, raw: SpecMap) -> ShellSpec:
    pipe = raw.get("pipe") or {}
    if not isinstance(pipe, dict):
        pipe = {}
    pipe_config = normalize_pipe_config(cast(dict[str, object], dict(cast(dict[object, object], pipe))))

    backend = normalize_launch_backend(str(raw.get("backend") or "proc"))
    command = raw.get("command")
    if not command:
        terminal_stream_mode = (
            backend == "pipe"
            and pipe_config.mode in {
                NATIVE_TERMINAL_PIPE_TESTING_MODE,
                PYTHON_TERMINAL_PIPE_TESTING_MODE,
            }
        )
        if terminal_stream_mode:
            if (
                pipe_config.mode == NATIVE_TERMINAL_PIPE_TESTING_MODE
                and pipe_config.terminal_fallback == TERMINAL_FALLBACK_COMMAND
            ):
                raise ValueError(
                    f"shellspec '{shell_id}' missing command for pipe.terminal_fallback=command"
                )
            command = [NATIVE_TERMINAL_PLACEHOLDER_COMMAND]
        else:
            raise ValueError(f"shellspec '{shell_id}' missing command")
    if not isinstance(command, (str, list)):
        raise ValueError(f"shellspec '{shell_id}' command must be string or list")
    if isinstance(command, list) and not all(isinstance(x, (str, int, float)) for x in command):
        raise ValueError(f"shellspec '{shell_id}' command list must be scalars")

    env_raw = raw.get("env") or {}
    if not isinstance(env_raw, dict):
        raise ValueError(f"shellspec '{shell_id}' env must be a mapping")

    subgroups = raw.get("subgroups") or []
    if not isinstance(subgroups, list):
        subgroups = []

    ui = raw.get("ui") or {}
    if not isinstance(ui, dict):
        ui = {}

    debug = raw.get("debug") or {}
    if not isinstance(debug, dict):
        debug = {}

    return ShellSpec(
        id=str(raw.get("id") or shell_id),
        command=command if isinstance(command, str) else [str(x) for x in command],
        cwd=_string_or_none(raw.get("cwd")),
        env={str(k): str(v) for k, v in cast(dict[object, object], env_raw).items()},
        subgroups=[str(x) for x in subgroups],
        ui=cast(dict[str, object], dict(cast(dict[object, object], ui))),
        debug=cast(dict[str, object], dict(cast(dict[object, object], debug))),
        pipe=cast(dict[str, object], dict(cast(dict[object, object], pipe))),
        pty_mode=str(raw.get("pty_mode") or raw.get("ptyMode") or "raw"),
        readiness=_parse_readiness(raw.get("readiness")),
        restart=_parse_restart(raw.get("restart")),
        backend=backend,
        autostart=bool(raw.get("autostart", True)),
    )


def parse_shellspec_data(raw: object, *, default_id: str | None = None) -> dict[str, ShellSpec]:
    """Parse an in-memory shellspec document into a mapping of id -> ShellSpec.

    Supported shapes:
    - Compose-like: {version: "1", shells: {id: {...}}}
    - Single-shell: {id: "...", command: ...} (id optional; defaults to `default_id`)
    """
    if not isinstance(raw, dict):
        return {}
    raw_map = _as_spec_map(cast(object, raw))

    shells_value = raw_map.get("shells")
    if isinstance(shells_value, dict):
        out: dict[str, ShellSpec] = {}
        for shell_id, shell_def in cast(dict[object, object], shells_value).items():
            if not isinstance(shell_def, dict):
                continue
            spec = _spec_from_dict(str(shell_id), cast(SpecMap, shell_def))
            out[spec.id] = spec
        return out

    # Single-shell format
    if "command" in raw_map:
        shell_id = str(raw_map.get("id") or default_id or "shell")
        return {shell_id: _spec_from_dict(shell_id, raw_map)}

    return {}


def load_shellspec(path: str | Path) -> dict[str, ShellSpec]:
    p = Path(path)
    if not p.exists():
        return {}

    if p.is_dir():
        merged: dict[str, ShellSpec] = {}
        for child in sorted(p.iterdir()):
            if child.suffix.lower() not in (".yaml", ".yml"):
                continue
            part = load_shellspec(child)
            for sid, spec in part.items():
                if sid in merged:
                    raise ValueError(f"duplicate shellspec id '{sid}' in {p}")
                merged[sid] = spec
        return merged

    with p.open("r", encoding="utf-8") as f:
        raw = cast(object, yaml.safe_load(f))
    return parse_shellspec_data(raw, default_id=p.stem)


def parse_shellspec_ref(ref: str) -> tuple[str, str | None]:
    """Parse `path[#id]` references."""
    if "#" not in ref:
        return ref, None
    path_part, shell_id = ref.split("#", 1)
    shell_id = shell_id.strip() or None
    return path_part.strip(), shell_id
