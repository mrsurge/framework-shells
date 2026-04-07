from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
import os
from pathlib import Path
import shutil
from types import ModuleType
from typing import Protocol, cast

NATIVE_PIPE_TESTING_MODE = "native_pipe_testing"
NATIVE_TERMINAL_PIPE_TESTING_MODE = "native_terminal_pipe_testing"
NATIVE_TERMINAL_PIPE_ENGINE = "native-terminal-pipe"
NATIVE_TERMINAL_PLACEHOLDER_COMMAND = "__fws_native_terminal_pipe__"
NATIVE_TERMINAL_BROKER_ENV = "FRAMEWORK_SHELLS_NATIVE_TERMINAL_BROKER"
NATIVE_TERMINAL_BROKER_BIN = "fws-terminal-stream-broker"
PIPE_PROFILE_LOW_LATENCY = "low_latency"
PIPE_PROFILE_BALANCED = "balanced"
PIPE_PROFILE_HIGH_THROUGHPUT = "high_throughput"
_SUPPORTED_PIPE_PROFILES = frozenset(
    {
        PIPE_PROFILE_LOW_LATENCY,
        PIPE_PROFILE_BALANCED,
        PIPE_PROFILE_HIGH_THROUGHPUT,
    }
)


@dataclass(frozen=True)
class NativePipeConfig:
    mode: str | None = None
    profile: str = PIPE_PROFILE_BALANCED
    read_chunk_bytes: int | None = None
    log_flush_bytes: int | None = None
    log_flush_interval_ms: int | None = None


class NativePipePumpHandle(Protocol):
    def stop(self) -> None:
        ...

    def drain_chunks(self, max_items: int | None = None) -> list[bytes]:
        ...

    def stats(self) -> dict[str, object]:
        ...

    def is_finished(self) -> bool:
        ...

    def wait_for_chunks(self, max_items: int | None = None, timeout_ms: int = 0) -> list[bytes]:
        ...


@dataclass(frozen=True)
class NativeTerminalBrokerResolution:
    command: list[str]
    engine: str | None = None
    source: str | None = None


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _int_or_none(value: object) -> int | None:
    if not isinstance(value, (str, int, float)):
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_pipe_config(raw: Mapping[str, object] | None) -> NativePipeConfig:
    if not raw:
        return NativePipeConfig()

    mode = _string_or_none(raw.get("mode"))
    profile = _string_or_none(raw.get("profile")) or PIPE_PROFILE_BALANCED
    if profile not in _SUPPORTED_PIPE_PROFILES:
        profile = PIPE_PROFILE_BALANCED

    return NativePipeConfig(
        mode=mode,
        profile=profile,
        read_chunk_bytes=_int_or_none(raw.get("read_chunk_bytes")),
        log_flush_bytes=_int_or_none(raw.get("log_flush_bytes")),
        log_flush_interval_ms=_int_or_none(raw.get("log_flush_interval_ms")),
    )


def _load_native_module() -> ModuleType | None:
    for module_name in ("fws_pipe_pump",):
        try:
            return import_module(module_name)
        except Exception:
            continue
    return None


_NATIVE_MODULE = _load_native_module()


def native_extension_available() -> bool:
    return _NATIVE_MODULE is not None


def native_extension_phase() -> str | None:
    module = _NATIVE_MODULE
    if module is None:
        return None
    phase = getattr(module, "__phase__", None)
    return phase if isinstance(phase, str) else None


def create_native_pipe_pump(
    *,
    stdout_fd: int,
    log_path: str,
    read_chunk_bytes: int,
    log_flush_bytes: int,
    log_flush_interval_ms: int,
) -> NativePipePumpHandle | None:
    module = _NATIVE_MODULE
    if module is None:
        return None
    pump_cls = getattr(module, "NativePipePump", None)
    if pump_cls is None:
        return None
    handle = pump_cls(
        int(stdout_fd),
        str(log_path),
        int(read_chunk_bytes),
        int(log_flush_bytes),
        int(log_flush_interval_ms),
    )
    return cast(NativePipePumpHandle, handle)


def _candidate_terminal_broker_paths() -> list[Path]:
    package_root = Path(__file__).resolve().parent.parent
    return [
        package_root / "framework_shells" / "bin" / NATIVE_TERMINAL_BROKER_BIN,
        package_root / "native" / "fws_terminal_stream_broker" / "target" / "release" / NATIVE_TERMINAL_BROKER_BIN,
        package_root / "native" / "fws_terminal_stream_broker" / "target" / "debug" / NATIVE_TERMINAL_BROKER_BIN,
    ]


def resolve_native_terminal_broker_command(
    fallback_command: list[str] | tuple[str, ...],
) -> NativeTerminalBrokerResolution:
    fallback = [str(part) for part in fallback_command]

    override = os.environ.get(NATIVE_TERMINAL_BROKER_ENV, "").strip()
    if override:
        path = Path(override).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return NativeTerminalBrokerResolution(
                command=[str(path)],
                engine=NATIVE_TERMINAL_PIPE_ENGINE,
                source=f"env:{NATIVE_TERMINAL_BROKER_ENV}",
            )

    for path in _candidate_terminal_broker_paths():
        if path.is_file() and os.access(path, os.X_OK):
            return NativeTerminalBrokerResolution(
                command=[str(path)],
                engine=NATIVE_TERMINAL_PIPE_ENGINE,
                source=str(path),
            )

    which_path = shutil.which(NATIVE_TERMINAL_BROKER_BIN)
    if which_path:
        return NativeTerminalBrokerResolution(
            command=[which_path],
            engine=NATIVE_TERMINAL_PIPE_ENGINE,
            source=f"PATH:{which_path}",
        )

    return NativeTerminalBrokerResolution(command=fallback)


def is_native_terminal_placeholder_command(
    command: list[str] | tuple[str, ...],
) -> bool:
    return len(command) == 1 and str(command[0]) == NATIVE_TERMINAL_PLACEHOLDER_COMMAND
