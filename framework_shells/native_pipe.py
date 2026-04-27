from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
import os
from pathlib import Path
import shutil
import sys
from types import ModuleType
from typing import Protocol, cast

NATIVE_PIPE_TESTING_MODE = "native_pipe_testing"
NATIVE_TERMINAL_PIPE_TESTING_MODE = "native_terminal_pipe_testing"
NATIVE_TERMINAL_PIPE_ENGINE = "native-terminal-pipe"
PYTHON_TERMINAL_PIPE_TESTING_MODE = "python_terminal_pipe_testing"
PYTHON_TERMINAL_PIPE_ENGINE = "python-terminal-pipe"
NATIVE_TERMINAL_PLACEHOLDER_COMMAND = "__fws_native_terminal_pipe__"
NATIVE_TERMINAL_BROKER_ENV = "FRAMEWORK_SHELLS_NATIVE_TERMINAL_BROKER"
NATIVE_TERMINAL_BROKER_BIN = "fws-terminal-stream-broker"
PYTHON_TERMINAL_BROKER_MODULE = "framework_shells.terminal_stream_broker"
TERMINAL_FALLBACK_PYTHON_PTY = "python_pty"
TERMINAL_FALLBACK_COMMAND = "command"
TERMINAL_FALLBACK_ERROR = "error"
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
_SUPPORTED_TERMINAL_FALLBACKS = frozenset(
    {
        TERMINAL_FALLBACK_PYTHON_PTY,
        TERMINAL_FALLBACK_COMMAND,
        TERMINAL_FALLBACK_ERROR,
    }
)


@dataclass(frozen=True)
class NativePipeConfig:
    mode: str | None = None
    profile: str = PIPE_PROFILE_BALANCED
    read_chunk_bytes: int | None = None
    log_flush_bytes: int | None = None
    log_flush_interval_ms: int | None = None
    terminal_fallback: str = TERMINAL_FALLBACK_PYTHON_PTY


class NativePipePumpHandle(Protocol):
    def stop(self) -> None:
        ...

    def read_available(self, max_items: int | None = None) -> list[bytes]:
        ...

    def reader_fd(self) -> int:
        ...

    def stats(self) -> dict[str, object]:
        ...

    def is_finished(self) -> bool:
        ...


class NativePipePumpFactory(Protocol):
    def __call__(
        self,
        stdout_fd: int,
        log_path: str,
        read_chunk_bytes: int,
        log_flush_bytes: int,
        log_flush_interval_ms: int,
    ) -> NativePipePumpHandle:
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
    terminal_fallback = normalize_terminal_fallback(raw.get("terminal_fallback"))

    return NativePipeConfig(
        mode=mode,
        profile=profile,
        read_chunk_bytes=_int_or_none(raw.get("read_chunk_bytes")),
        log_flush_bytes=_int_or_none(raw.get("log_flush_bytes")),
        log_flush_interval_ms=_int_or_none(raw.get("log_flush_interval_ms")),
        terminal_fallback=terminal_fallback,
    )


def normalize_terminal_fallback(value: object) -> str:
    text = _string_or_none(value)
    if text in {"native_only", "none"}:
        return TERMINAL_FALLBACK_ERROR
    if text in _SUPPORTED_TERMINAL_FALLBACKS:
        return text
    return TERMINAL_FALLBACK_PYTHON_PTY


def _load_native_module() -> ModuleType | None:
    for module_name in ("framework_shells.fws_pipe_pump", "fws_pipe_pump"):
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
    pump_cls = cast(NativePipePumpFactory | None, getattr(module, "NativePipePump", None))
    if pump_cls is None:
        return None
    return pump_cls(
        stdout_fd=int(stdout_fd),
        log_path=str(log_path),
        read_chunk_bytes=int(read_chunk_bytes),
        log_flush_bytes=int(log_flush_bytes),
        log_flush_interval_ms=int(log_flush_interval_ms),
    )


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


def resolve_python_terminal_broker_command() -> list[str]:
    return [sys.executable, "-m", PYTHON_TERMINAL_BROKER_MODULE]


def resolve_terminal_broker_fallback_command(
    fallback_mode: object,
    shellspec_command: list[str] | tuple[str, ...],
) -> list[str] | None:
    normalized = normalize_terminal_fallback(fallback_mode)
    if normalized == TERMINAL_FALLBACK_PYTHON_PTY:
        return resolve_python_terminal_broker_command()
    if normalized == TERMINAL_FALLBACK_COMMAND:
        command = [str(part) for part in shellspec_command]
        if command and not is_native_terminal_placeholder_command(command):
            return command
        return None
    return None


def is_native_terminal_placeholder_command(
    command: list[str] | tuple[str, ...],
) -> bool:
    return len(command) == 1 and str(command[0]) == NATIVE_TERMINAL_PLACEHOLDER_COMMAND
