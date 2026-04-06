from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from types import ModuleType

NATIVE_PIPE_TESTING_MODE = "native_pipe_testing"
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


class NativePipePumpHandle:
    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def poll_chunks(self) -> list[bytes]:
        raise NotImplementedError

    def stats(self) -> dict[str, object]:
        raise NotImplementedError


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
